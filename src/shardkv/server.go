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
  ServerLog = false
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

  table map[string]string
  cache map[string]*Result
  lastAppliedSeq int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  key := args.Key
  clientId,_ := ParseReqId(args.ReqId)

  kv.log("Get receive, key=%s, clientId=%s", key, clientId)

  op := Op{Operation: Get, Args: *args, ReqId: args.ReqId}

  val,err := kv.resolveOp(op)
  reply.Value = val
  reply.Err = err

  kv.log("Get return, key=%s, val=%s, clientId=%s", key, val, clientId)

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  key := args.Key
  val := args.Value
  clientId,_ := ParseReqId(args.ReqId)

  kv.log("Put receive, key=%s, val=%s, clientId=%s", key, val, clientId)

  op := Op{Operation: Put, Args: *args, ReqId: args.ReqId}

  prev,err := kv.resolveOp(op)
  reply.PreviousValue = prev
  reply.Err = err

  kv.log("Put return, key=%s, val=%s, prev=%s, clientId=%s", key, val, prev, clientId)

  return nil
}

func (kv *ShardKV) resolveOp(op Op) (string,Err) {
  seq := kv.px.Max()+1
  clientId,_ := ParseReqId(op.ReqId)

  kv.mu.Lock()
  dup,exists := kv.cache[op.ReqId]
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

  kv.log("Seq=%d decided!", seq)

  // block until seq op has been applied
  for kv.lastAppliedSeq < seq {
    time.Sleep(InitTimeout)
  }

  kv.mu.Lock()
  result,exists := kv.cache[op.ReqId]
  kv.mu.Unlock()

  kv.px.Done(seq)

  if !exists {
    // should not happen since assuming at most one outstanding get/put
    error := fmt.Sprintf("Couldn't find result for op=%s, clientId=%s", op.Operation, clientId)
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

func (kv *ShardKV) applyOp(op *Op) (string,Err) {
  // Return early for a noop
  if op.Operation == Noop {
    return "",ErrNoOp
  }

  // Check if operation was already applied
  kv.mu.Lock()
  _,exists := kv.cache[op.ReqId]
  kv.mu.Unlock()

  if exists {
    kv.log("Already applied %d", op.ReqId)
    return "",ErrAlreadyApplied
  }

  switch op.Operation {
  case Get:
    args := op.Args.(GetArgs)
    return kv.applyGet(args.Key)
  case Put:
    args := op.Args.(PutArgs)
    return kv.applyPut(args.Key, args.Value, args.DoHash)
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
      if err == OK || err == ErrNoKey {
        kv.cache[op.ReqId] = &Result{ReqId: op.ReqId, Val: val, Err: err}
      }

      kv.lastAppliedSeq += 1
      kv.mu.Unlock()

      kv.log("Apply %s from seq=%d", op.Operation, seq)

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

func (kv *ShardKV) reconfig() {
  // op := Op{Operation: Reconfig}


}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  // Query shardmaster for latest config
  newConfig := kv.sm.Query(-1)

  // Reconfigure if there's a new configuration
  if newConfig.Num > kv.config.Num {
    kv.reconfig()
  }
}

func (kv *ShardKV) log(format string, a ...interface{}) (n int, err error) {
  if ServerLog {
    addr := "Srv#" + strconv.Itoa(kv.me)
    gid := "GID#" + strconv.FormatInt(kv.gid, 10)
    n, err = fmt.Printf(addr + "|" + gid + " >> " + format + "\n", a...)
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

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.config = kv.sm.Query(-1)

  kv.table = make(map[string]string)
  kv.cache = make(map[string]*Result)
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
