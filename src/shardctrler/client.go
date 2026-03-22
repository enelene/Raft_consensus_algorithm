package shardctrler

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers     []*labrpc.ClientEnd
	uniqueID    int64
	sequenceNum int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.uniqueID = nrand()
	ck.sequenceNum = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	request := RequestPayload{
		ClientId:    ck.uniqueID,
		SequenceNum: ck.sequenceNum,
		Method:      "Query",
		Num:         num,
	}
	return ck.sendRequest(request)
}

func (ck *Clerk) Join(servers map[int][]string) {
	request := RequestPayload{
		ClientId:    ck.uniqueID,
		SequenceNum: ck.sequenceNum,
		Method:      "Join",
		Servers:     servers,
	}
	ck.sendRequest(request)
}

func (ck *Clerk) Leave(gids []int) {
	request := RequestPayload{
		ClientId:    ck.uniqueID,
		SequenceNum: ck.sequenceNum,
		Method:      "Leave",
		GIDs:        gids,
	}
	ck.sendRequest(request)
}

func (ck *Clerk) Move(shard int, gid int) {
	request := RequestPayload{
		ClientId:    ck.uniqueID,
		SequenceNum: ck.sequenceNum,
		Method:      "Move",
		MoveShard:   shard,
		MoveGID:     gid,
	}
	ck.sendRequest(request)
}

// sendRequest sends an operation request to the shard controller servers and waits for a response
// no need to do same thing in all above 4 functions.
func (ck *Clerk) sendRequest(request RequestPayload) Config {
	for {
		for _, server := range ck.servers {
			response := ResponsePayload{}
			ok := server.Call("ShardCtrler.Operation", &request, &response)
			if ok && response.Err == OK {
				ck.uniqueID += 1
				return response.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
