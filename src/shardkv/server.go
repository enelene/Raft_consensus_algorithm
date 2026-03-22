package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

/****************************
   Constants and Types
****************************/

type Configuration struct {
	Config shardctrler.Config
}

const (
	Default    = 0
	Pull       = 1
	Push       = 2
	Collection = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	MessageId int
	Key       string
	Value     string
	Method    string
}

type Shard struct {
	Status int
	State  map[string]string
}

type Snapshot struct {
	State      [shardctrler.NShards]Shard
	Config     shardctrler.Config
	LastConfig shardctrler.Config
	Client     map[int64]int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister   *raft.Persister
	dead        int32
	curconfig   shardctrler.Config
	prevConfig  shardctrler.Config
	clerk       *shardctrler.Clerk
	state       [shardctrler.NShards]Shard
	clients     map[int64]int
	broadcastCh map[int]chan interface{}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("ShardKV[%d|GID=%d] is killed\n", kv.me, kv.gid)
}

/****************************
    Main Apply Loop
****************************/

func (kv *ShardKV) applyLoop() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		DPrintf("ShardKV[%d|GID=%d] applyLoop: received applyMsg: %+v\n", kv.me, kv.gid, applyMsg)

		if applyMsg.CommandValid {
			kv.processCommand(applyMsg)
		} else if applyMsg.SnapshotValid {
			DPrintf("ShardKV[%d|GID=%d] applyLoop: SnapshotValid -> apply snapshot\n", kv.me, kv.gid)
			kv.readPersist(applyMsg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

/*
***************************

	RPC Handlers

***************************
*/

func (kv *ShardKV) PullShard(request *PullShardRequest, response *PullShardResponse) {
	kv.mu.Lock()
	DPrintf("ShardKV[%d|GID=%d] PullShard RPC: shardList=%v, req.Num=%d, curCfg.Num=%d\n",
		kv.me, kv.gid, request.ShardList, request.Num, kv.curconfig.Num)
	kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.curconfig.Num < request.Num {
		response.Err = ErrFuture
		kv.mu.Unlock()
		return
	}

	state := make(map[int]Shard)
	clientCp := make(map[int64]int)

	for _, sh := range request.ShardList {
		cp := make(map[string]string)
		for k, v := range kv.state[sh].State {
			cp[k] = v
		}
		state[sh] = Shard{State: cp}
	}

	for cID, msgID := range kv.clients {
		clientCp[cID] = msgID
	}

	response.State = state
	response.Client = clientCp
	response.Num = kv.curconfig.Num
	response.Err = OK
	DPrintf("ShardKV[%d|GID=%d] PullShard => OK, returning shard data\n", kv.me, kv.gid)
	kv.mu.Unlock()
}

func (kv *ShardKV) DeleteShard(request *DeleteShardRequest, response *DeleteShardResponse) {
	kv.mu.Lock()
	DPrintf("ShardKV[%d|GID=%d] DeleteShard RPC: shardList=%v, req.Num=%d, curCfg.Num=%d\n",
		kv.me, kv.gid, request.ShardList, request.Num, kv.curconfig.Num)
	kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.curconfig.Num > request.Num {
		response.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*request)
	if isLeader {
		_, response.Err = kv.waitForConsensus(index)
	} else {
		response.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) Operation(request *PutAppendArgs, response *PutAppendReply) {
	kv.mu.Lock()
	shard := key2shard(request.Key)
	DPrintf("ShardKV[%d|GID=%d] Operation RPC: key=%s, op=%s, shard=%d\n",
		kv.me, kv.gid, request.Key, request.Op, shard)
	if kv.isstaleShard(shard) {
		DPrintf("ShardKV[%d|GID=%d] Operation -> stale shard => ErrWrongGroup\n", kv.me, kv.gid)
		response.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClientId:  request.ClientId,
		MessageId: request.MessageId,
		Key:       request.Key,
		Value:     request.Value,
		Method:    request.Op,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("ShardKV[%d|GID=%d] Operation -> not leader => ErrWrongLeader\n", kv.me, kv.gid)
		response.Err = ErrWrongLeader
		return
	}

	appliedCommand, err := kv.waitForConsensus(index)
	if err == ErrTimeout {
		DPrintf("ShardKV[%d|GID=%d] Operation -> consensus timeout => ErrTimeout\n", kv.me, kv.gid)
		response.Err = ErrTimeout
		return
	}

	operation, ok := appliedCommand.(Op)
	if !ok {
		DPrintf("ShardKV[%d|GID=%d] Operation -> mismatch type => ErrWrongLeader\n", kv.me, kv.gid)
		response.Err = ErrWrongLeader
		return
	}

	appliedClientId := operation.ClientId
	appliedMessageId := operation.MessageId
	if operation.ClientId != appliedClientId || operation.MessageId != appliedMessageId {
		DPrintf("ShardKV[%d|GID=%d] Operation -> client mismatch => ErrWrongLeader\n", kv.me, kv.gid)
		response.Err = ErrWrongLeader
		return
	}
	// Finally re-check
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.isstaleShard(shard) {
		response.Err = ErrWrongGroup
		return
	}
	if request.Op == "Get" {
		response.Value = kv.state[shard].State[request.Key]
		DPrintf("ShardKV[%d|GID=%d] Operation -> Get => OK, value=%s\n", kv.me, kv.gid, response.Value)
	}
	response.Err = OK
	DPrintf("ShardKV[%d|GID=%d] Operation => OK\n", kv.me, kv.gid)
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	DPrintf("ShardKV[%d|GID=%d] readPersist: applying snapshot\n", kv.me, kv.gid)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var snap Snapshot

	if d.Decode(&snap) != nil {
		log.Printf("ShardKV[%d|GID=%d]: snapshot decode error\n", kv.me, kv.gid)
		return
	}
	kv.state = snap.State
	kv.curconfig = snap.Config
	kv.prevConfig = snap.LastConfig
	kv.clients = snap.Client
}

func (kv *ShardKV) configLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		defaultShard := true
		for _, shard := range kv.state {
			if shard.Status != Default {
				defaultShard = false
				break
			}
		}
		num := kv.curconfig.Num
		kv.mu.Unlock()

		_, isLeader := kv.rf.GetState()
		if isLeader && defaultShard {
			nextConfig := kv.clerk.Query(num + 1)
			nextNum := nextConfig.Num

			if num+1 == nextNum {
				command := Configuration{
					Config: nextConfig,
				}
				index, _, isLeader := kv.rf.Start(command)
				if isLeader {
					kv.waitForConsensus(index)
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) pullshardLoop() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			gidShardList := make(map[int][]int)
			for shard := range kv.state {
				if kv.state[shard].Status == Pull {
					gid := kv.prevConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.Unlock()

			var waitGroup sync.WaitGroup
			for gid, shardList := range gidShardList {
				waitGroup.Add(1)
				go kv.sendPullShard(&waitGroup, gid, shardList)
			}
			waitGroup.Wait()
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) deleteShardLoop() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			gidShardList := make(map[int][]int)
			for shard := range kv.state {
				if kv.state[shard].Status == Collection {
					gid := kv.prevConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.Unlock()

			var waitGroup sync.WaitGroup
			for gid, shardList := range gidShardList {
				waitGroup.Add(1)
				go kv.sendDeleteShard(&waitGroup, gid, shardList)
			}
			waitGroup.Wait()
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) heartbeatLoop() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			index, _, isLeader := kv.rf.Start(Op{})
			if isLeader {
				kv.waitForConsensus(index)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

/****************************
    Raft Notification
****************************/

func (kv *ShardKV) waitForConsensus(index int) (interface{}, Err) {
	kv.mu.Lock()
	if _, ok := kv.broadcastCh[index]; !ok {
		kv.broadcastCh[index] = make(chan interface{}, 1)
	}
	ch := kv.broadcastCh[index]
	DPrintf("ShardKV[%d|GID=%d] waitForConsensus => waiting on idx=%d\n", kv.me, kv.gid, index)
	kv.mu.Unlock()

	select {
	case cmd := <-ch:
		DPrintf("ShardKV[%d|GID=%d] waitForConsensus => idx=%d success\n", kv.me, kv.gid, index)
		return cmd, OK
	case <-time.After(500 * time.Millisecond):
		DPrintf("ShardKV[%d|GID=%d] waitForConsensus => idx=%d timed out\n", kv.me, kv.gid, index)
		return nil, ErrTimeout
	}
}

/****************************
   Shard Transfer Helpers
****************************/

func (kv *ShardKV) isstaleShard(sh int) bool {
	cfgOwner := kv.curconfig.Shards[sh]
	st := kv.state[sh].Status
	DPrintf("ShardKV[%d|GID=%d] isstaleShard? shard=%d configOwner=%v shardStatus=%v",
		kv.me, kv.gid, sh, cfgOwner, st)

	if cfgOwner != kv.gid {
		DPrintf("ShardKV[%d|GID=%d] -> stale reason: configOwner != myGid",
			kv.me, kv.gid)
		return true
	}
	if st == Pull || st == Push {
		DPrintf("ShardKV[%d|GID=%d] -> stale reason: status == %v",
			kv.me, kv.gid, st)
		return true
	}
	return false
}

func (kv *ShardKV) sendPullShard(s *sync.WaitGroup, gid int, list []int) {
	defer s.Done()
	DPrintf("ShardKV[%d|GID=%d] sendPullShard => to GID=%d shards=%v\n", kv.me, kv.gid, gid, list)

	for _, server := range kv.prevConfig.Groups[gid] {
		request := PullShardRequest{
			ShardList: list,
			Num:       kv.curconfig.Num,
		}
		rsp := PullShardResponse{}
		ok := kv.make_end(server).Call("ShardKV.PullShard", &request, &rsp)
		if ok && rsp.Err == OK {
			DPrintf("ShardKV[%d|GID=%d] sendPullShard => got OK from server=%s, applying\n", kv.me, kv.gid, server)
			idx, _, leader := kv.rf.Start(rsp)
			if leader {
				kv.waitForConsensus(idx)
			}
			break
		} else {
			DPrintf("ShardKV[%d|GID=%d] sendPullShard => server=%s returned err=%v\n", kv.me, kv.gid, server, rsp.Err)
		}
	}
}

func (kv *ShardKV) sendDeleteShard(s *sync.WaitGroup, gid int, list []int) {
	defer s.Done()
	DPrintf("ShardKV[%d|GID=%d] sendDeleteShard => to GID=%d shards=%v\n", kv.me, kv.gid, gid, list)

	for _, srv := range kv.prevConfig.Groups[gid] {
		req := DeleteShardRequest{
			ShardList: list,
			Num:       kv.curconfig.Num,
		}
		rsp := DeleteShardResponse{}
		ok := kv.make_end(srv).Call("ShardKV.DeleteShard", &req, &rsp)
		if ok && rsp.Err == OK {
			DPrintf("ShardKV[%d|GID=%d] sendDeleteShard => got OK from server=%s, applying\n", kv.me, kv.gid, srv)
			idx, _, leader := kv.rf.Start(req)
			if leader {
				kv.waitForConsensus(idx)
			}
			break
		} else {
			DPrintf("ShardKV[%d|GID=%d] sendDeleteShard => server=%s returned err=%v\n", kv.me, kv.gid, srv, rsp.Err)
		}
	}
}

/*
***************************

	Command Processing

***************************
*/

func (kv *ShardKV) processCommand(applyMsg raft.ApplyMsg) {
	idx := applyMsg.CommandIndex
	switch cmd := applyMsg.Command.(type) {

	case Op:
		DPrintf("ShardKV[%d|GID=%d] processCommand: Op: %+v\n", kv.me, kv.gid, cmd)
		sh := key2shard(cmd.Key)
		// Check if new and not stale
		if !kv.isstaleShard(sh) && kv.clients[cmd.ClientId] < cmd.MessageId {
			if cmd.Method == "Put" {
				DPrintf("ShardKV[%d|GID=%d] Put key=%s val=%s shard=%d\n", kv.me, kv.gid, cmd.Key, cmd.Value, sh)
				kv.state[sh].State[cmd.Key] = cmd.Value
			} else if cmd.Method == "Append" {
				DPrintf("ShardKV[%d|GID=%d] Append key=%s val=%s shard=%d\n", kv.me, kv.gid, cmd.Key, cmd.Value, sh)
				kv.state[sh].State[cmd.Key] += cmd.Value
			}
			kv.clients[cmd.ClientId] = cmd.MessageId
		}

	case Configuration:
		DPrintf("ShardKV[%d|GID=%d] processCommand: Configuration\n", kv.me, kv.gid)
		nextCfg := cmd.Config
		if nextCfg.Num == kv.curconfig.Num+1 {
			DPrintf("ShardKV[%d|GID=%d] applying config from num=%d to num=%d\n", kv.me, kv.gid, kv.curconfig.Num, nextCfg.Num)
			for shard, newGid := range nextCfg.Shards {
				oldGid := kv.curconfig.Shards[shard]
				if newGid == kv.gid && oldGid != kv.gid && oldGid != 0 {
					DPrintf("ShardKV[%d] changing shard %d status to Pull\n", kv.me, shard)
					kv.state[shard].Status = Pull
				}
				if newGid != kv.gid && oldGid == kv.gid && newGid != 0 {
					DPrintf("ShardKV[%d] changing shard %d status to Push\n", kv.me, shard)
					kv.state[shard].Status = Push
				}
			}
			kv.prevConfig = kv.curconfig
			kv.curconfig = nextCfg
		} else {
			DPrintf("ShardKV[%d|GID=%d] ignoring out-of-order config: nextCfgNum=%d, curConfigNum=%d\n",
				kv.me, kv.gid, nextCfg.Num, kv.curconfig.Num)
		}

	case PullShardResponse:
		DPrintf("ShardKV[%d|GID=%d] processCommand: PullShard\n", kv.me, kv.gid)
		if cmd.Num == kv.curconfig.Num {
			for shardID := range cmd.State {
				if kv.state[shardID].Status == Pull {
					DPrintf("ShardKV[%d|GID=%d] merging pulled shard %d\n", kv.me, kv.gid, shardID)
					for k, v := range cmd.State[shardID].State {
						kv.state[shardID].State[k] = v
					}
					kv.state[shardID].Status = Collection
				}
			}
			// update client requests
			for cID, msgID := range cmd.Client {
				if kv.clients[cID] < msgID {
					kv.clients[cID] = msgID
				}
			}
		} else {
			DPrintf("ShardKV[%d|GID=%d] ignoring PullShardResponse with num=%d (cur=%d)\n",
				kv.me, kv.gid, cmd.Num, kv.curconfig.Num)
		}

	case DeleteShardRequest:
		DPrintf("ShardKV[%d|GID=%d] processCommand: DeleteShardRequest\n", kv.me, kv.gid)
		if cmd.Num == kv.curconfig.Num {
			for _, sh := range cmd.ShardList {
				if kv.state[sh].Status == Collection {
					DPrintf("ShardKV[%d] shard %d leaving Collection -> Default\n", kv.me, sh)
					kv.state[sh].Status = Default
				}
				if kv.state[sh].Status == Push {
					DPrintf("ShardKV[%d] shard %d was Push -> resetting\n", kv.me, sh)
					kv.state[sh] = Shard{Status: Default, State: make(map[string]string)}
				}
			}
		} else {
			DPrintf("ShardKV[%d|GID=%d] ignoring DeleteShardRequest with num=%d (cur=%d)\n",
				kv.me, kv.gid, cmd.Num, kv.curconfig.Num)
		}

	default:
		DPrintf("ShardKV[%d|GID=%d] processCommand: unknown type %T\n", kv.me, kv.gid, cmd)
	}

	if broadcastCh, ok := kv.broadcastCh[applyMsg.CommandIndex]; ok {
		DPrintf("ShardKV[%d|GID=%d] processCommand: notifying broadcastCh for index=%d\n", kv.me, kv.gid, idx)
		broadcastCh <- applyMsg.Command
	}

	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("ShardKV[%d|GID=%d] taking snapshot at index=%d (RaftStateSize=%d)\n",
			kv.me, kv.gid, applyMsg.CommandIndex, kv.persister.RaftStateSize())
		snapshot := Snapshot{
			State:      kv.state,
			Config:     kv.curconfig,
			LastConfig: kv.prevConfig,
			Client:     kv.clients,
		}

		var buf bytes.Buffer
		enc := labgob.NewEncoder(&buf)
		if err := enc.Encode(snapshot); err != nil {
			log.Fatalf("ShardKV[%d] encode snapshot error: %v\n", kv.me, err)
		}
		kv.rf.Snapshot(applyMsg.CommandIndex, buf.Bytes())
	}

}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Configuration{})
	labgob.Register(PullShardResponse{})
	labgob.Register(DeleteShardRequest{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.clients = make(map[int64]int)
	kv.broadcastCh = make(map[int]chan interface{})
	for index := range kv.state {
		kv.state[index] = Shard{
			Status: Default,
			State:  make(map[string]string),
		}
	}

	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.applyLoop()
	go kv.configLoop()
	go kv.pullshardLoop()
	go kv.deleteShardLoop()
	go kv.heartbeatLoop()

	return kv
}
