package raftkv

import (
	"bytes"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string // get | put | append
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data   map[string]string // key-value data
	ack    map[int64]int64   // client's latest request id (for deduplication)
	notify map[int]chan Op   // log index to notifying chan (for checking status)
}

//
// try to append an entry to raft servers' log.
// return true if raft servers apply this entry before timeout.
//
func (kv *KVServer) appendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.notify[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.notify[index] = ch
	}
	kv.mu.Unlock()

	select {
	case appliedEntry := <-ch:
		return entry == appliedEntry
	case <-time.After(240 * time.Millisecond):
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

//
// Apply operation on database.
//
func (kv *KVServer) applyOp(op Op) {
	switch op.Command {
	case "put":
		kv.data[op.Key] = op.Value
	case "append":
		kv.data[op.Key] += op.Value
	}
	kv.ack[op.ClientId] = op.RequestId
}

//
// Apply snapshot on database.
//
func (kv *KVServer) applySnapshot(snapshot []byte) {
	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&kv.data)
	d.Decode(&kv.ack)
}

//
// Check if the request is duplicated with request id.
//
func (kv *KVServer) isDuplicate(op Op) bool {
	latestRequestId, ok := kv.ack[op.ClientId]
	if ok {
		return latestRequestId >= op.RequestId
	}
	return false
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
		kv.mu.Lock()

		if msg.UseSnapshot {
			kv.applySnapshot(msg.Snapshot)
		} else {
			// apply operation if it is not duplicate request
			op := msg.Command.(Op)
			if !kv.isDuplicate(op) {
				kv.applyOp(op)
			}

			// send success notification (even for duplicate request)
			ch, ok := kv.notify[msg.CommandIndex]
			if ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				ch = make(chan Op, 1)
				kv.notify[msg.CommandIndex] = ch
			}
			ch <- op

			// create snapshot if raft state exceeds allowed size
			if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.ack)
				go kv.rf.CreateSnapshot(w.Bytes(), msg.CommandIndex)
			}
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
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.notify = make(map[int]chan Op)

	go kv.Run()
	return kv
}
