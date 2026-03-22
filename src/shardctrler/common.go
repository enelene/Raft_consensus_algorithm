package shardctrler

const NShards = 10

type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK         = "OK"
	ErrTimeout = "ErrTimeout"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type ResponsePayload struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type RequestPayload struct {
	Method      string
	ClientId    int64
	SequenceNum int
	Servers     map[int][]string // new GID -> servers mappings
	GIDs        []int
	MoveShard   int
	MoveGID     int
	Num         int // desired config number
}
