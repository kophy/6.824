package shardkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command string
	// Parameters for key-value service.
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
	// Parameters for updating configuration.
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
}

type Result struct {
	Command     string
	OK          bool
	WrongLeader bool
	Err         Err
	// Parameters for key-value service.
	ClientId  int64
	RequestId int64
	Value     string
	// Parameters for updating configuration.
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     [shardmaster.NShards]map[string]string
	ack      map[int64]int64
	resultCh map[int]chan Result
	config   shardmaster.Config
	mck      *shardmaster.Clerk
}

//
// try to append the entry to raft servers' log and return result.
// result is valid if raft servers apply this entry before timeout.
//
func (kv *ShardKV) appendEntryToLog(entry Op) Result {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.resultCh[index]; !ok {
		kv.resultCh[index] = make(chan Result, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultCh[index]:
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
	if entry.Command == "reconfigure" {
		return entry.Config.Num == result.ConfigNum
	}
	return entry.ClientId == result.ClientId && entry.RequestId == result.RequestId
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "get"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Key = args.Key

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{}
	entry.Command = args.Op
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Key = args.Key
	entry.Value = args.Value

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (kv *ShardKV) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.RequestId = op.RequestId

	switch op.Command {
	case "put":
		kv.applyPut(op, &result)
	case "append":
		kv.applyAppend(op, &result)
	case "get":
		kv.applyGet(op, &result)
	case "reconfigure":
		kv.applyReconfigure(op, &result)
	}
	return result
}

func (kv *ShardKV) applyPut(args Op, result *Result) {
	if !kv.isValidKey(args.Key) {
		result.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.data[key2shard(args.Key)][args.Key] = args.Value
		kv.ack[args.ClientId] = args.RequestId
	}
	result.Err = OK
}

func (kv *ShardKV) applyAppend(args Op, result *Result) {
	if !kv.isValidKey(args.Key) {
		result.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.data[key2shard(args.Key)][args.Key] += args.Value
		kv.ack[args.ClientId] = args.RequestId
	}
	result.Err = OK
}

func (kv *ShardKV) applyGet(args Op, result *Result) {
	if !kv.isValidKey(args.Key) {
		result.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.ack[args.ClientId] = args.RequestId
	}
	if value, ok := kv.data[key2shard(args.Key)][args.Key]; ok {
		result.Value = value
		result.Err = OK
	} else {
		result.Err = ErrNoKey
	}
}

func (kv *ShardKV) applyReconfigure(args Op, result *Result) {
	result.ConfigNum = args.Config.Num
	if args.Config.Num == kv.config.Num+1 {
		for shardId, shardData := range args.Data {
			for k, v := range shardData {
				kv.data[shardId][k] = v
			}
		}
		// merge ack map from args to this server's ack map.
		for clientId := range args.Ack {
			if _, ok := kv.ack[clientId]; !ok || kv.ack[clientId] < args.Ack[clientId] {
				kv.ack[clientId] = args.Ack[clientId]
			}
		}
		kv.config = args.Config
	}
	result.Err = OK
}

//
// check if the request is duplicated with request id.
//
func (kv *ShardKV) isDuplicated(op Op) bool {
	lastRequestId, ok := kv.ack[op.ClientId]
	if ok {
		return lastRequestId >= op.RequestId
	}
	return false
}

//
// check if the key is handled by this replica group.
//
func (kv *ShardKV) isValidKey(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) Run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.UseSnapshot {
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)

			var lastIncludedIndex, lastIncludedTerm int
			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)
			d.Decode(&kv.data)
			d.Decode(&kv.ack)
			d.Decode(&kv.config)
		} else {
			// apply operation and send result
			op := msg.Command.(Op)
			result := kv.applyOp(op)
			if ch, ok := kv.resultCh[msg.CommandIndex]; ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				kv.resultCh[msg.CommandIndex] = make(chan Result, 1)
			}
			kv.resultCh[msg.CommandIndex] <- result

			// create snapshot if raft state exceeds allowed size
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.ack)
				e.Encode(kv.config)
				go kv.rf.CreateSnapshot(w.Bytes(), msg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

type TransferShardArgs struct {
	Num      int
	ShardIds []int
}

type TransferShardReply struct {
	Err  Err
	Data [shardmaster.NShards]map[string]string
	Ack  map[int64]int64
}

//
// if this server is ready, copy shards required in args and ack to reply.
//
func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.Num {
		// this server is not ready (may still handle the requested shards).
		reply.Err = ErrNotReady
		return
	}

	// copy shards and ack to reply.
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}
	for _, shardId := range args.ShardIds {
		for k, v := range kv.data[shardId] {
			reply.Data[shardId][k] = v
		}
	}
	reply.Ack = make(map[int64]int64)
	for clientId, requestId := range kv.ack {
		reply.Ack[clientId] = requestId
	}
	reply.Err = OK
}

//
// try to get shards requested in args and ack from replica group specified by gid.
//
func (kv *ShardKV) sendTransferShard(gid int, args *TransferShardArgs, reply *TransferShardReply) bool {
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.TransferShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return true
}

type ReconfigureArgs struct {
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
}

//
// collect shards from other replica groups required for reconfiguration and build
// reconfigure log entry to append to raft log.
//
func (kv *ShardKV) getReconfigureEntry(nextConfig shardmaster.Config) (Op, bool) {
	entry := Op{}
	entry.Command = "reconfigure"
	entry.Config = nextConfig
	for i := 0; i < shardmaster.NShards; i++ {
		entry.Data[i] = make(map[string]string)
	}
	entry.Ack = make(map[int64]int64)
	ok := true

	var ackMu sync.Mutex
	var wg sync.WaitGroup

	transferShards := kv.getShardsToTransfer(nextConfig)
	for gid, shardIds := range transferShards {
		wg.Add(1)

		go func(gid int, args TransferShardArgs, reply TransferShardReply) {
			defer wg.Done()

			if kv.sendTransferShard(gid, &args, &reply) {
				ackMu.Lock()
				// copy only shards requested from that replica group to reconfigure args
				for _, shardId := range args.ShardIds {
					shardData := reply.Data[shardId]
					for k, v := range shardData {
						entry.Data[shardId][k] = v
					}
				}
				// merge ack map from that replica group to reconfigure args
				for clientId := range reply.Ack {
					if _, ok := entry.Ack[clientId]; !ok || entry.Ack[clientId] < reply.Ack[clientId] {
						entry.Ack[clientId] = reply.Ack[clientId]
					}
				}
				ackMu.Unlock()
			} else {
				ok = false
			}
		}(gid, TransferShardArgs{Num: nextConfig.Num, ShardIds: shardIds}, TransferShardReply{})
	}
	wg.Wait()
	return entry, ok
}

//
// build a map from gid to shard ids to request from replica group specified by gid.
//
func (kv *ShardKV) getShardsToTransfer(nextConfig shardmaster.Config) map[int][]int {
	transferShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.config.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := transferShards[gid]; !ok {
					transferShards[gid] = make([]int, 0)
				}
				transferShards[gid] = append(transferShards[gid], i)
			}
		}
	}
	return transferShards
}

//
// if this server is leader of the replica group , it should query latest
// configuration periodically and try to update configuration if rquired.
//
func (kv *ShardKV) Reconfigure() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			latestConfig := kv.mck.Query(-1)
			for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
				// apply configuration changes in order
				nextConfig := kv.mck.Query(i)
				entry, ok := kv.getReconfigureEntry(nextConfig)
				if !ok {
					break
				}
				result := kv.appendEntryToLog(entry)
				if !result.OK {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Result{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	kv.ack = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Result)
	kv.mck = shardmaster.MakeClerk(masters)

	go kv.Run()
	go kv.Reconfigure()

	return kv
}
