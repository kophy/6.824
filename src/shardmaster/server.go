package shardmaster

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	// Your data here.
	ack      map[int64]int64     // client's latest request id (for deduplication)
	resultCh map[int]chan Result // log index to notifying chan (for checking status)
}

type Op struct {
	// Your data here.
	Command   string
	ClientId  int64
	RequestId int64
	// JoinArgs
	Servers map[int][]string
	// LeaveArgs
	GIDs []int
	// MoveArgs
	Shard int
	GID   int
	// QueryArgs
	Num int
}

type Result struct {
	Command     string
	OK          bool
	ClientId    int64
	RequestId   int64
	WrongLeader bool
	Err         Err
	// QueryReply
	Config Config
}

//
// try to append the entry to raft servers' log and return result.
// result is valid if raft servers apply this entry before timeout.
//
func (sm *ShardMaster) appendEntryToLog(entry Op) Result {
	index, _, isLeader := sm.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	sm.mu.Lock()
	if _, ok := sm.resultCh[index]; !ok {
		sm.resultCh[index] = make(chan Result, 1)
	}
	sm.mu.Unlock()

	select {
	case result := <-sm.resultCh[index]:
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(240 * time.Millisecond):
		return Result{OK: false}
	}
}

//
// check if the result corresponds to the log entry.
//
func isMatch(entry Op, result Result) bool {
	return entry.ClientId == result.ClientId && entry.RequestId == result.RequestId
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "join"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Servers = args.Servers

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "leave"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.GIDs = args.GIDs

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "move"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Shard = args.Shard
	entry.GID = args.GID

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "query"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Num = args.Num

	result := sm.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Config = result.Config
}

//
// apply operation on configuration and return result.
//
func (sm *ShardMaster) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.RequestId = op.RequestId

	switch op.Command {
	case "join":
		if !sm.isDuplicated(op) {
			sm.applyJoin(op)
		}
		result.Err = OK
	case "leave":
		if !sm.isDuplicated(op) {
			sm.applyLeave(op)
		}
		result.Err = OK
	case "move":
		if !sm.isDuplicated(op) {
			sm.applyMove(op)
		}
		result.Err = OK
	case "query":
		if op.Num == -1 || op.Num >= len(sm.configs) {
			result.Config = sm.configs[len(sm.configs)-1]
		} else {
			result.Config = sm.configs[op.Num]
		}
		result.Err = OK
	}
	sm.ack[op.ClientId] = op.RequestId
	return result
}

//
// check if the request is duplicated with request id.
//
func (sm *ShardMaster) isDuplicated(op Op) bool {
	lastRequestId, ok := sm.ack[op.ClientId]
	if ok {
		return lastRequestId >= op.RequestId
	}
	return false
}

//
// make a new configuration from current configuration.
// all fields are same as current configuration except config number.
//
func (sm *ShardMaster) makeNextConfig() Config {
	nextConfig := Config{}
	currConfig := sm.configs[len(sm.configs)-1]

	nextConfig.Num = currConfig.Num + 1
	nextConfig.Shards = currConfig.Shards
	nextConfig.Groups = map[int][]string{}
	for gid, servers := range currConfig.Groups {
		nextConfig.Groups[gid] = servers
	}
	return nextConfig
}

//
// apply join request to modify config.
//
func (sm *ShardMaster) applyJoin(args Op) {
	config := sm.makeNextConfig()
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers

		// assign shards without replica group to this replica group.
		for i := 0; i < NShards; i++ {
			if config.Shards[i] == 0 {
				config.Shards[i] = gid
			}
		}
	}
	rebalanceShards(&config)
	sm.configs = append(sm.configs, config)
}

//
// apply leave request to modify config.
//
func (sm *ShardMaster) applyLeave(args Op) {
	config := sm.makeNextConfig()
	// find a replica group won't leave.
	stayGid := 0
	for gid := range config.Groups {
		stay := true
		for _, deletedGid := range args.GIDs {
			if gid == deletedGid {
				stay = false
			}
		}
		if stay {
			stayGid = gid
			break
		}
	}

	for _, gid := range args.GIDs {
		// assign shards whose replica group will leave to the stay group.
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == gid {
				config.Shards[i] = stayGid
			}
		}
		delete(config.Groups, gid)
	}
	rebalanceShards(&config)
	sm.configs = append(sm.configs, config)
}

//
// apply move request to modify config.
//
func (sm *ShardMaster) applyMove(args Op) {
	config := sm.makeNextConfig()
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)
}

//
// rebalance shards among replica groups.
//
func rebalanceShards(config *Config) {
	gidToShards := makeGidToShards(config)
	if len(config.Groups) == 0 {
		// no replica group
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
	} else {
		mean := NShards / len(config.Groups)
		numToMove := 0
		for _, shards := range gidToShards {
			if len(shards) > mean {
				numToMove += len(shards) - mean
			}
		}
		for i := 0; i < numToMove; i++ {
			// each time move a shard from replica group with most shards to replica
			// gorup with fewest shards.
			srcGid, dstGid := getGidPairToMove(gidToShards)
			N := len(gidToShards[srcGid]) - 1
			config.Shards[gidToShards[srcGid][N]] = dstGid
			gidToShards[dstGid] = append(gidToShards[dstGid], gidToShards[srcGid][N])
			gidToShards[srcGid] = gidToShards[srcGid][:N]
		}
	}
}

//
// make a map from replica gorup id to the shards they handle.
//
func makeGidToShards(config *Config) map[int][]int {
	gidToShards := make(map[int][]int)
	for gid := range config.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	return gidToShards
}

//
// find a pair of gids (srcGid, dstGid) to move one shard.
// here srcGid replica group has most shards, while dstGid replica group has
// fewest shards.
//
func getGidPairToMove(gidToShards map[int][]int) (int, int) {
	srcGid, dstGid := 0, 0
	for gid, shards := range gidToShards {
		if srcGid == 0 || len(gidToShards[srcGid]) < len(shards) {
			srcGid = gid
		}
		if dstGid == 0 || len(gidToShards[dstGid]) > len(shards) {
			dstGid = gid
		}
	}
	return srcGid, dstGid
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) Run() {
	for {
		msg := <-sm.applyCh
		sm.mu.Lock()
		// apply operation and send result
		op := msg.Command.(Op)
		result := sm.applyOp(op)
		if ch, ok := sm.resultCh[msg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			sm.resultCh[msg.CommandIndex] = make(chan Result, 1)
		}
		sm.resultCh[msg.CommandIndex] <- result
		sm.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Result{})

	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.ack = make(map[int64]int64)
	sm.resultCh = make(map[int]chan Result)

	go sm.Run()
	return sm
}
