package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "log"

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

	// Your data here.
	ack    map[int64]int64
	notify map[int]chan Op

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Command   string
	ClientId  int64
	RequestId int64
	Args      interface{}
}

func (sm *ShardMaster) getLatestConfig() *Config {
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
	ch, ok := sm.notify[index]
	if !ok {
		ch = make(chan Op, 1)
		sm.notify[index] = ch
	}
	sm.mu.Unlock()

	select {
	case appliedEntry := <-ch:
		return entry.ClientId == appliedEntry.ClientId && entry.RequestId == appliedEntry.RequestId
	case <-time.After(240 * time.Millisecond):
		return false
	}
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
// Apply operation on configuration.
//
func (sm *ShardMaster) applyOp(op Op) {
	switch op.Command {
	case "join":
		args := op.Args.(JoinArgs)
		sm.makeNextConfig()
		sm.applyJoin(args)
	case "leave":
		args := op.Args.(LeaveArgs)
		sm.makeNextConfig()
		sm.applyLeave(args)
	case "move":
		args := op.Args.(MoveArgs)
		sm.makeNextConfig()
		sm.applyMove(args)
	case "get":
	}
	sm.ack[op.ClientId] = op.RequestId
}

func (sm *ShardMaster) makeNextConfig() {
	config := Config{}
	config.Num = len(sm.configs)
	config.Shards = sm.configs[len(sm.configs)-1].Shards
	config.Groups = map[int][]string{}
	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		config.Groups[k] = v
	}
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) makeGidToShards() map[int][]int {
	config := sm.getLatestConfig()
	gidToShards := make(map[int][]int)
	for gid := range config.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	return gidToShards
}

func getGidWithMaxShards(gidToShards map[int][]int) int {
	maxGid := -1
	for gid, shards := range gidToShards {
		if maxGid == -1 || len(gidToShards[maxGid]) < len(shards) {
			maxGid = gid
		}
	}
	return maxGid
}

func getGidWithMinShards(gidToShards map[int][]int) int {
	minGid := -1
	for gid, shards := range gidToShards {
		if minGid == -1 || len(gidToShards[minGid]) > len(shards) {
			minGid = gid
		}
	}
	return minGid
}

func (sm *ShardMaster) applyJoin(args JoinArgs) {
	config := sm.getLatestConfig()
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers
		for i := range config.Shards {
			if config.Shards[i] == 0 {
				config.Shards[i] = gid
			}
		}
	}
	gidToShards := sm.makeGidToShards()
	if len(config.Groups) == 0 {
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
			maxGid := getGidWithMaxShards(gidToShards)
			minGid := getGidWithMinShards(gidToShards)
			config.Shards[gidToShards[maxGid][0]] = minGid
			gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
			gidToShards[maxGid] = gidToShards[maxGid][1:]
		}
	}
}

func (sm *ShardMaster) applyLeave(args LeaveArgs) {
	config := sm.getLatestConfig()

	tempGid := 0
	for gid := range config.Groups {
		flag := true
		for _, deletedGid := range args.GIDs {
			if gid == deletedGid {
				flag = false
			}
		}
		if flag {
			tempGid = gid
			break
		}
	}

	for _, gid := range args.GIDs {
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == gid {
				config.Shards[i] = tempGid
			}
		}
		delete(config.Groups, gid)
	}

	gidToShards := sm.makeGidToShards()
	if len(config.Groups) == 0 {
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
			maxGid := getGidWithMaxShards(gidToShards)
			minGid := getGidWithMinShards(gidToShards)
			config.Shards[gidToShards[maxGid][0]] = minGid
			gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
			gidToShards[maxGid] = gidToShards[maxGid][1:]
		}
	}
}

func (sm *ShardMaster) applyMove(args MoveArgs) {
	config := sm.getLatestConfig()
	config.Shards[args.Shard] = args.GID
}

//
// Check if the request is duplicated with request id.
//
func (sm *ShardMaster) isDuplicate(op Op) bool {
	latestRequestId, ok := sm.ack[op.ClientId]
	if ok {
		return latestRequestId >= op.RequestId
	}
	return false
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
		ch, ok := sm.notify[msg.CommandIndex]
		if ok {
			select {
			case <-ch:
			default:
			}
		} else {
			ch = make(chan Op, 1)
			sm.notify[msg.CommandIndex] = ch
		}
		ch <- op
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
	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryReply{})
	sm.ack = make(map[int64]int64)
	sm.notify = make(map[int]chan Op)

	go sm.Run()

	return sm
}
