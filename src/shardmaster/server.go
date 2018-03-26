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
	ack    map[int64]int64 // client's latest request id (for deduplication)
	notify map[int]chan Op // log index to notifying chan (for checking status)
}

type Op struct {
	// Your data here.
	Command   string
	ClientId  int64
	RequestId int64
	Args      interface{} // save whole request args for simplicity.
}

func (sm *ShardMaster) getCurrentConfig() *Config {
	return &sm.configs[len(sm.configs)-1]
}

//
// try to append an entry to raft servers' log.
// return true if raft servers apply this entry before timeout.
//
func (sm *ShardMaster) appendEntryToLog(entry Op) bool {
	index, _, isLeader := sm.rf.Start(entry)
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	if _, ok := sm.notify[index]; !ok {
		sm.notify[index] = make(chan Op, 1)
	}
	sm.mu.Unlock()

	select {
	case appliedEntry := <-sm.notify[index]:
		return isEqualEntry(entry, appliedEntry)
	case <-time.After(240 * time.Millisecond):
		return false
	}
}

//
// check if two entries are equal(with same client id and request id).
//
func isEqualEntry(entry1 Op, entry2 Op) bool {
	return entry1.ClientId == entry2.ClientId && entry1.RequestId == entry2.RequestId
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "join"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Args = *args

	ok := sm.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "leave"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Args = *args

	ok := sm.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "move"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Args = *args

	ok := sm.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "query"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Args = *args

	ok := sm.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
	sm.mu.Lock()
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	sm.mu.Unlock()
}

//
// apply operation on configuration.
//
func (sm *ShardMaster) applyOp(op Op) {
	switch op.Command {
	case "join":
		args := op.Args.(JoinArgs)
		config := sm.makeNextConfig()
		applyJoin(&config, args)
		sm.configs = append(sm.configs, config)
	case "leave":
		args := op.Args.(LeaveArgs)
		config := sm.makeNextConfig()
		applyLeave(&config, args)
		sm.configs = append(sm.configs, config)
	case "move":
		args := op.Args.(MoveArgs)
		config := sm.makeNextConfig()
		applyMove(&config, args)
		sm.configs = append(sm.configs, config)
	case "get":
	}
	sm.ack[op.ClientId] = op.RequestId
}

//
// make a new configuration from current configuration.
// all fields are same as current configuration except config number.
//
func (sm *ShardMaster) makeNextConfig() Config {
	nextConfig := Config{}
	currConfig := sm.getCurrentConfig()

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
func applyJoin(config *Config, args JoinArgs) {
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers

		// assign shards without replica group to this replica group.
		for i := 0; i < NShards; i++ {
			if config.Shards[i] == 0 {
				config.Shards[i] = gid
			}
		}
	}
	rebalanceShards(config)
}

//
// apply leave request to modify config.
//
func applyLeave(config *Config, args LeaveArgs) {
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
	rebalanceShards(config)
}

//
// apply move request to modify config.
//
func applyMove(config *Config, args MoveArgs) {
	config.Shards[args.Shard] = args.GID
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
			config.Shards[gidToShards[srcGid][0]] = dstGid
			gidToShards[dstGid] = append(gidToShards[dstGid], gidToShards[srcGid][0])
			gidToShards[srcGid] = gidToShards[srcGid][1:]
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
		op := msg.Command.(Op)

		sm.mu.Lock()
		if !sm.isDuplicate(op) {
			// apply operation if it is not duplicate request
			sm.applyOp(op)
		}

		// send success notification (even for duplicate request)
		if ch, ok := sm.notify[msg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			sm.notify[msg.CommandIndex] = make(chan Op, 1)
		}
		sm.notify[msg.CommandIndex] <- op
		sm.mu.Unlock()
	}
}

//
// check if the request is duplicated with request id.
//
func (sm *ShardMaster) isDuplicate(op Op) bool {
	if latestRequestId, ok := sm.ack[op.ClientId]; ok {
		return latestRequestId >= op.RequestId
	}
	return false
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
	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.ack = make(map[int64]int64)
	sm.notify = make(map[int]chan Op)

	go sm.Run()
	return sm
}
