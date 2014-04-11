package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "strings"

const (
  ServerLog = true
  InitTimeout = 10 * time.Millisecond
)

func ParseReqId(reqId string) (string,uint64) {
  s := strings.Split(reqId, ":")
  clientId := s[0]
  reqNum,_ := strconv.ParseUint(s[1], 10, 64)
  return clientId,reqNum
}

func RandMTime() time.Duration {
  return time.Duration(rand.Int() % 100)  * time.Millisecond
}

func CorrectGroup(key string, gid int64, config shardmaster.Config) bool {
  shard := key2shard(key)
  if shard >= shardmaster.NShards {
    return false
  }
  return config.Shards[shard] == gid
}

func CopyTable(src map[string]string) map[string]string {
  dst := make(map[string]string)
  for k,v := range src {
    dst[k] = v
  }
  return dst
}

const (
  Put = "Put"
  Get = "Get"
  Reconfig = "Reconfig"
  Noop = "Noop"
)
type Operation string

type Op struct {
  Operation Operation
  Args interface{}
  ReqId string
  ConfigNum int
}

type Result struct {
  ReqId string
  Val string
  Err Err
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID
  config shardmaster.Config
  transfer sync.Mutex

  table map[string]string // key -> value
  tableCache map[int]map[string]string // confignum -> key -> value
  reqs map[string]*Result
  lastAppliedSeq int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.transfer.Lock()
  defer kv.transfer.Unlock()

  key := args.Key
  reqId := args.ReqId

  kv.log("Get receive, key=%s, reqId=%s", key, reqId)

  if !CorrectGroup(key, kv.gid, kv.config) {
    kv.log("Wrong group")
    reply.Err = ErrWrongGroup
    return nil
  }

  op := Op{Operation: Get, Args: *args, ReqId: reqId, ConfigNum: kv.config.Num}

  val,err := kv.resolveOp(op)
  reply.Value = val
  reply.Err = err

  kv.log("Get return, key=%s, val=%s, reqId=%s", key, val, reqId)

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.transfer.Lock()
  defer kv.transfer.Unlock()

  key := args.Key
  val := args.Value
  reqId := args.ReqId

  kv.log("Put receive, key=%s, val=%s, reqId=%s", key, val, reqId)

  if !CorrectGroup(key, kv.gid, kv.config) {
    kv.log("Wrong group")
    reply.Err = ErrWrongGroup
    return nil
  }

  op := Op{Operation: Put, Args: *args, ReqId: reqId, ConfigNum: kv.config.Num}

  prev,err := kv.resolveOp(op)
  reply.PreviousValue = prev
  reply.Err = err

  kv.log("Put return, key=%s, val=%s, prev=%s, reqId=%s", key, val, prev, reqId)

  return nil
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) error {

  kv.mu.Lock()
  defer kv.mu.Unlock()

  configNum := args.ConfigNum
  shard := args.Shard
  // kv.log("Transfer receive, config=%d, shard=%d", configNum, shard)

  cache,exists := kv.tableCache[configNum]

  if !exists {
    // kv.log("wrong view %d", kv.config.Num)
    reply.Err = ErrWrongView
    return nil
  }

  result := make(map[string]string)
  for k,v := range cache {
    if key2shard(k) == shard {
      result[k] = v
    }
  }
  reply.Table = result
  reply.Reqs = kv.reqs
  reply.Err = OK

  // kv.log("Transfer return, config=%d, shard=%d", configNum, shard)

  return nil
}

func (kv *ShardKV) resolveOp(op Op) (string,Err) {
  seq := kv.px.Max()+1

  kv.mu.Lock()
  dup,exists := kv.reqs[op.ReqId]
  kv.mu.Unlock()

  if exists {
    return dup.Val, dup.Err
  }

  kv.px.Start(seq, op)

  to := InitTimeout
  time.Sleep(to)

  decided,val := kv.px.Status(seq)
  for !decided || val != op {
    if (decided && val != op) || (seq <= kv.lastAppliedSeq) {
      kv.log("Seq=%d already decided", seq)
      seq = kv.px.Max()+1
      kv.px.Start(seq, op)
    }

    // kv.log("Retry w/ seq=%d", seq)
    time.Sleep(to + RandMTime())
    if to < 100 * time.Millisecond {
      to *= 2
    }

    decided,val = kv.px.Status(seq)
  }

  // kv.log("Seq=%d decided!", seq)

  // block until seq op has been applied
  for kv.lastAppliedSeq < seq {
    time.Sleep(InitTimeout)
  }

  kv.mu.Lock()
  result,exists := kv.reqs[op.ReqId]
  kv.mu.Unlock()

  kv.px.Done(seq)

  if !exists {
    // should not happen since assuming at most one outstanding get/put
    error := fmt.Sprintf("Couldn't find result for op=%s, reqId=%s", op.Operation, op.ReqId)
    panic(error)
    return "", ErrInvalid
  }

  return result.Val, result.Err
}

func (kv *ShardKV) applyGet(key string) (string,Err) {
  val, ok := kv.table[key]

  if !ok {
    return "",ErrNoKey
  }

  return val,OK
}

func (kv *ShardKV) applyPut(key string, val string, dohash bool) (string,Err) {
  oldval, ok := kv.table[key]
  if !ok {
    oldval = ""
  }

  newval := val
  if dohash {
    newval = strconv.Itoa(int(hash(oldval + newval)))
  }

  kv.table[key] = newval

  return oldval,OK
}

func (kv *ShardKV) applyReconfig(fromConfigNum int, toConfigNum int) (string,Err) {

  old := kv.sm.Query(fromConfigNum)
  next := kv.sm.Query(toConfigNum)

  kv.config = next

  // Skip the initial blank config
  if fromConfigNum == 0 {
    return "",OK
  }

  kv.tableCache[old.Num] = CopyTable(kv.table)

  for shard,newGid := range next.Shards {
    oldGid := old.Shards[shard]
    if oldGid != newGid && newGid == kv.gid {
      // request shards from replicas in another group
      fetched := false
      for !fetched && kv.dead == false {
        for _,server := range old.Groups[oldGid] {
          args := &TransferArgs{ConfigNum: old.Num, Shard: shard}
          var reply TransferReply
          ok := call(server, "ShardKV.Transfer", args, &reply)
          // kv.log("transfer rpc ok=%s, server=%s", ok, server)
          if ok && reply.Err == OK {
            // kv.log("table = %s", reply.Table)
            kv.mergeTable(reply.Table)
            kv.mergeReqs(reply.Reqs)
            fetched = true
            break
          }

          time.Sleep(100 * time.Millisecond)
        }
      }
    }
  }

  return "",OK
}

func (kv *ShardKV) mergeTable(incoming map[string]string) {
  for k,v := range incoming {
    kv.table[k] = v
  }
}

func (kv *ShardKV) mergeReqs(incoming map[string]*Result) {
  for k,v := range incoming {
    kv.reqs[k] = v
  }
}

func (kv *ShardKV) applyOp(op *Op) (string,Err) {
  // Return early for a noop
  if op.Operation == Noop {
    return "",ErrNoOp
  }

  // Check if operation was already applied
  kv.mu.Lock()
  _,exists := kv.reqs[op.ReqId]
  kv.mu.Unlock()

  if exists {
    return "",ErrAlreadyApplied
  }

  if op.Operation != Reconfig && op.ConfigNum != kv.config.Num {
    return "",ErrWrongGroup
  }

  switch op.Operation {
  case Get:
    args := op.Args.(GetArgs)
    return kv.applyGet(args.Key)
  case Put:
    args := op.Args.(PutArgs)
    return kv.applyPut(args.Key, args.Value, args.DoHash)
  case Reconfig:
    args := op.Args.(ReconfigArgs)
    return kv.applyReconfig(args.FromConfigNum, args.ToConfigNum)
  }

  // Should not reach this point
  return "",ErrInvalid
}

func (kv *ShardKV) logSync() {

  timeout := InitTimeout

  for kv.dead == false {

    seq := kv.lastAppliedSeq+1
    decided,result := kv.px.Status(seq)

    if decided {
      // apply the operation
      op,_ := result.(Op)
      val,err := kv.applyOp(&op)

      kv.mu.Lock()
      if err == OK || err == ErrNoKey || err == ErrWrongGroup {
        kv.reqs[op.ReqId] = &Result{ReqId: op.ReqId, Val: val, Err: err}
      }

      kv.lastAppliedSeq += 1
      kv.mu.Unlock()

      if err != ErrAlreadyApplied {
        // kv.log("Applied %s of reqId=%s", op.Operation, op.ReqId)
      } else {
        // kv.log("Already applied %s", op.ReqId)
      }

      // reset timeout
      timeout = InitTimeout

    } else {
      // kv.log("Retry for seq=%d", seq)

      if timeout >= 1 * time.Second {
        kv.log("Try noop for seq=%d", seq)
        kv.px.Start(seq, Op{Operation: Noop})

        // wait for noop to return
        noopDone := false
        for !noopDone {
          noopDone,_ = kv.px.Status(seq)
          time.Sleep(100 * time.Millisecond)
        }
      } else {
        // wait before retrying
        time.Sleep(timeout)

        if timeout < 1 * time.Second {
          // expotential backoff
          timeout *= 2
        }
      }
    }
  }
}

func (kv *ShardKV) prepareReconfig(oldConfig shardmaster.Config, newConfig shardmaster.Config) {
  if oldConfig.Num >= newConfig.Num {
    return
  }

  reqId := strconv.FormatInt(kv.gid, 10) + "|" + strconv.Itoa(oldConfig.Num) + "->" + strconv.Itoa(newConfig.Num)
  _,exists := kv.reqs[reqId]
  if exists {
    return
  }

  args := ReconfigArgs{FromConfigNum: oldConfig.Num, ToConfigNum: newConfig.Num}
  op := Op{Operation: Reconfig, Args: args, ReqId: reqId, ConfigNum: oldConfig.Num}

  kv.resolveOp(op)

}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.transfer.Lock()
  defer kv.transfer.Unlock()

  // Query shardmaster for latest config
  newConfig := kv.sm.Query(-1)

  // kv.log("tick: current=%d, latest=%d", kv.config.Num, newConfig.Num)

  for (newConfig.Num - kv.config.Num) >= 1 {

    kv.prepareReconfig(kv.config, kv.sm.Query(kv.config.Num+1))

    newConfig = kv.sm.Query(-1)
  }
}

func (kv *ShardKV) log(format string, a ...interface{}) (n int, err error) {
  if ServerLog {
    addr := "Srv#" + strconv.Itoa(kv.me) + "|"
    gid := "GID#" + strconv.FormatInt(kv.gid, 10) + "|"
    config := "Config#" + strconv.Itoa(kv.config.Num)
    n, err = fmt.Printf(addr + gid + config + " >> " + format + "\n", a...)
  }
  return
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(PutArgs{})
  gob.Register(GetArgs{})
  gob.Register(ReconfigArgs{})
  gob.Register(TransferArgs{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.config = kv.sm.Query(-1)

  kv.table = make(map[string]string)
  kv.tableCache = make(map[int]map[string]string)
  kv.reqs = make(map[string]*Result)
  kv.lastAppliedSeq = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  go kv.logSync()

  return kv
}
