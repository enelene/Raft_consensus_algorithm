package shardctrler

import (
	"6.5840/raft"
	"sort"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	persister   *raft.Persister
	broadcastCh map[int]chan RequestPayload // Channels for notifying client requests
	configs     []Config
	clients     map[int64]int
	dead        int32
}

// apply listens for applied messages from Raft and processes commands
func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh // Wait for an applied message
		sc.mu.Lock()

		if applyMsg.CommandValid {
			command := applyMsg.Command.(RequestPayload)
			commandIndex := applyMsg.CommandIndex

			SequenceNum := command.SequenceNum
			clientId := command.ClientId
			method := command.Method

			// Process the command only if it's new for the client
			if sc.clients[clientId] < SequenceNum {
				latestConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{
					Num:    latestConfig.Num + 1,
					Groups: make(map[int][]string),
				}

				for gid, servers := range latestConfig.Groups {
					newConfig.Groups[gid] = append([]string{}, servers...)
				}
				copy(newConfig.Shards[:], latestConfig.Shards[:])

				if method == "Move" {
					newConfig.Shards[command.MoveShard] = command.MoveGID
				} else if method == "Join" {
					for gid, servers := range command.Servers {
						newConfig.Groups[gid] = servers
					}
					rebalanceShards(&newConfig)
				} else if method == "Leave" {
					for _, gid := range command.GIDs {
						delete(newConfig.Groups, gid)
					}
					rebalanceShards(&newConfig)
				}

				if method == "Join" || method == "Leave" {
					GIDs := make([]int, 0, len(newConfig.Groups))
					for gid := range newConfig.Groups {
						GIDs = append(GIDs, gid)
					}

					if len(GIDs) > 0 {
						sort.Ints(GIDs)
						for shard := range newConfig.Shards {
							newConfig.Shards[shard] = GIDs[shard%len(GIDs)]
						}
					} else {
						for shard := range newConfig.Shards {
							newConfig.Shards[shard] = 0
						}
					}
				}

				if method != "Query" {
					sc.configs = append(sc.configs, newConfig)
				}
				sc.clients[clientId] = SequenceNum
			}

			// Notify the client waiting on this index
			if broadcastCh, ok := sc.broadcastCh[commandIndex]; ok {
				broadcastCh <- command
			}
		}

		sc.mu.Unlock()
	}
}

// rebalanceShards reassigns shards minimally so that
// no group differs in its shard count by more than 1.
func rebalanceShards(cfg *Config) {
	activeGroups := make([]int, 0, len(cfg.Groups))
	for gid := range cfg.Groups {
		activeGroups = append(activeGroups, gid)
	}
	sort.Ints(activeGroups)

	// If no groups, assign shards to 0 and return.
	if len(activeGroups) == 0 {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	shardMap := make(map[int][]int)
	for _, gid := range activeGroups {
		shardMap[gid] = []int{}
	}
	for shard, gid := range cfg.Shards {
		if _, ok := cfg.Groups[gid]; !ok {
			cfg.Shards[shard] = 0
		} else {
			shardMap[gid] = append(shardMap[gid], shard)
		}
	}

	var unassigned []int
	for shard := 0; shard < NShards; shard++ {
		if cfg.Shards[shard] == 0 {
			unassigned = append(unassigned, shard)
		}
	}

	numGroups := len(activeGroups)
	minLoad := NShards / numGroups
	maxLoad := minLoad
	if NShards%numGroups != 0 {
		maxLoad = minLoad + 1
	}

	for _, gid := range activeGroups {
		for len(shardMap[gid]) > maxLoad {
			// Pop a shard from this gid to unassigned
			idx := len(shardMap[gid]) - 1
			s := shardMap[gid][idx]
			shardMap[gid] = shardMap[gid][:idx]
			unassigned = append(unassigned, s)
			cfg.Shards[s] = 0
		}
	}

	for _, gid := range activeGroups {
		// While this group is below minLoad and we have unassigned shards, assign
		for len(shardMap[gid]) < minLoad && len(unassigned) > 0 {
			s := unassigned[len(unassigned)-1]
			unassigned = unassigned[:len(unassigned)-1]
			shardMap[gid] = append(shardMap[gid], s)
			cfg.Shards[s] = gid
		}
	}

	for _, gid := range activeGroups {
		for len(shardMap[gid]) < maxLoad && len(unassigned) > 0 {
			s := unassigned[len(unassigned)-1]
			unassigned = unassigned[:len(unassigned)-1]
			shardMap[gid] = append(shardMap[gid], s)
			cfg.Shards[s] = gid
		}
	}

}

// Operation handles client requests by proposing them to Raft
func (sc *ShardCtrler) Operation(request *RequestPayload, response *ResponsePayload) {
	clientID := request.ClientId
	seqNum := request.SequenceNum
	queryNum := request.Num

	index, _, isLeader := sc.rf.Start(*request)
	if !isLeader {
		response.WrongLeader = true
		return
	}

	sc.mu.Lock()
	sc.broadcastCh[index] = make(chan RequestPayload, 1)
	broadcast := sc.broadcastCh[index]
	sc.mu.Unlock()

	select {
	case appliedCmd := <-broadcast:
		if appliedCmd.ClientId != clientID || appliedCmd.SequenceNum != seqNum {
			response.WrongLeader = true
			return
		}

		sc.mu.Lock()
		// Provide the requested configuration or the latest
		if queryNum == -1 || queryNum >= len(sc.configs) {
			response.Config = sc.configs[len(sc.configs)-1]
		} else {
			response.Config = sc.configs[queryNum]
		}
		sc.mu.Unlock()

		response.Err = OK
	case <-time.After(500 * time.Millisecond):
		response.Err = ErrTimeout
	}

	// Clean up the channel after use
	go func() {
		sc.mu.Lock()
		close(broadcast)
		delete(sc.broadcastCh, index)
		sc.mu.Unlock()
	}()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := &ShardCtrler{
		me:          me,
		persister:   persister,
		applyCh:     make(chan raft.ApplyMsg),
		clients:     make(map[int64]int),
		broadcastCh: make(map[int]chan RequestPayload),
		configs:     []Config{{Groups: map[int][]string{}}},
	}

	// Initialize the first configuration
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(RequestPayload{})
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Start the apply routine in a separate goroutine
	go sc.apply()
	return sc
}
