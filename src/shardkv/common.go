package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type ReconfigureArgs struct {
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
}

type ReconfigureReply struct {
	Err Err
	Num int
}

type TransferShardArgs struct {
	Num      int
	ShardIds []int
}

type TransferShardReply struct {
	Err         Err
	Data        [shardmaster.NShards]map[string]string
	Ack         map[int64]int64
}
