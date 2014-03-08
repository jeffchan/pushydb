package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"
import "strings"

const (
  Debug = 0
  InitTimeout = 10 * time.Millisecond
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

func ParseReqId(reqId string) (string,uint64) {
  s := strings.Split(reqId, ":")
  clientId := s[0]
  reqNum,_ := strconv.ParseUint(s[1], 10, 64)
  return clientId,reqNum
}

func randMTime() time.Duration {
  return time.Duration(rand.Int() % 100)  * time.Millisecond
}

type Op struct {
  Operation Operation
  ClientId string
  ReqId string
  Key string
  Value string
}

type Result struct {
  ReqId string
  Val string
  Err Err
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  table map[string]string
  cache map[string]*Result
  lastAppliedSeq int
}

const ServerLog = false
func (kv *KVPaxos) log(format string, a ...interface{}) (n int, err error) {
  if ServerLog {
    addr := "Srv#" + strconv.Itoa(kv.me)
    n, err = fmt.Printf(addr + ": " + format + "\n", a...)
  }
  return
}

func (kv *KVPaxos) applyOp(op *Op) (string,Err) {
  // Return early for a noop
  if op.Operation == Noop {
    return "",ErrNoOp
  }

  // Find last operation from same client
  kv.mu.Lock()
  lastClientOp,exists := kv.cache[op.ClientId]
  kv.mu.Unlock()

  if exists {
    _,lastReqNum := ParseReqId(lastClientOp.ReqId)
    _,reqNum := ParseReqId(op.ReqId)

    // Don't double apply any operation
    if reqNum <= lastReqNum {
      kv.log("Already applied %d", reqNum)
      return "",ErrAlreadyApplied
    }
  }

  switch op.Operation {
  case Get:
    return kv.applyGet(op.Key)
  case Put:
    return kv.applyPut(op.Key, op.Value, false)
  case PutHash:
    return kv.applyPut(op.Key, op.Value, true)
  }

  // Should not reach this point
  return "",ErrInvalid
}

func (kv *KVPaxos) applyGet(key string) (string,Err) {
  val, ok := kv.table[key]

  if !ok {
    return "",ErrNoKey
  }

  return val,OK
}

func (kv *KVPaxos) applyPut(key string, val string, dohash bool) (string,Err) {
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

func (kv *KVPaxos) resolveOp(op Op) (string,Err) {
  seq := kv.px.Max()+1
  clientId := op.ClientId

  kv.mu.Lock()
  dup,exists := kv.cache[clientId]
  kv.mu.Unlock()

  if exists && dup.ReqId == op.ReqId {
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
    time.Sleep(to + randMTime())
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
  result,exists := kv.cache[clientId]
  kv.mu.Unlock()

  kv.px.Done(seq)

  if !exists {
    // should not happen since assuming at most one outstanding get/put
    return "", ErrInvalid
  }

  return result.Val, result.Err
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  key := args.Key
  clientId,_ := ParseReqId(args.ReqId)

  kv.log("Get receive, key=%s, clientId=%s", key, clientId)

  op := Op{Operation: Get, ClientId: clientId, ReqId: args.ReqId, Key: key}

  val,err := kv.resolveOp(op)
  reply.Value = val
  reply.Err = err

  kv.log("Get return, key=%s, val=%s, clientId=%s", key, val, clientId)

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  key := args.Key
  val := args.Value
  clientId,_ := ParseReqId(args.ReqId)

  kv.log("Put receive, key=%s, val=%s, clientId=%s", key, val, clientId)

  var operation Operation
  if args.DoHash {
    operation = PutHash
  } else {
    operation = Put
  }

  op := Op{Operation: operation, ClientId: clientId, ReqId: args.ReqId, Key: key, Value: val}

  prev,err := kv.resolveOp(op)
  reply.PreviousValue = prev
  reply.Err = err

  kv.log("Put return, key=%s, val=%s, prev=%s, clientId=%s", key, val, prev, clientId)

  return nil
}

func (kv *KVPaxos) tick() {

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
        kv.cache[op.ClientId] = &Result{ReqId: op.ReqId, Val: val, Err: err}
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

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go kv.tick()

  return kv
}

