package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

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
	Command   string // get | put | append
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data   map[string]string
	ack    map[int64]int
	result map[int]chan Op
}

func (kv *KVServer) appendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		return op == entry
	case <-time.After(800 * time.Millisecond):
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "get"
	entry.Key = args.Key
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId

	ok := kv.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
	kv.mu.Lock()
	reply.Value = kv.data[args.Key]
	kv.ack[args.ClientId] = args.RequestId
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{}
	entry.Command = args.Command
	entry.Key = args.Key
	entry.Value = args.Value
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId

	ok := kv.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (kv *KVServer) applyEntry(entry Op) {
	switch entry.Command {
	case "put":
		kv.data[entry.Key] = entry.Value
		DPrintf("PUT (%s, %s), server = %d, log = %d", entry.Key, entry.Value, kv.me, kv.rf.GetLogLength())
	case "append":
		kv.data[entry.Key] += entry.Value
		DPrintf("ADD (%s, %s), server = %d, log = %d", entry.Key, kv.data[entry.Key], kv.me, kv.rf.GetLogLength())
	case "get":
		DPrintf("GET (%s, %s), server = %d, log = %d", entry.Key, kv.data[entry.Key], kv.me, kv.rf.GetLogLength())
	}
	kv.ack[entry.ClientId] = entry.RequestId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) Run() {
	for {
		msg := <-kv.applyCh
		op := msg.Command.(Op)

		kv.mu.Lock()

		requestId, ok := kv.ack[op.ClientId]
		if !ok || requestId < op.RequestId {
			// TODO(problem may be here!!!)
			kv.applyEntry(op)
			kv.ack[op.ClientId] = op.RequestId
		}

		ch, ok := kv.result[msg.CommandIndex]
		if ok {
			ch <- op
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	go kv.Run()
	return kv
}
