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

const (
  Debug = 0
  TickInterval = 100 * time.Millisecond
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Op struct {
  Operation Operation
  ReqId string
  Key string
  Value string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  table map[string]string
  cache map[int]string
  highestDone int
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
  switch op.Operation {
  case Get:
    return kv.applyGet(op.Key)
  case Put:
    return kv.applyPut(op.Key, op.Value, false)
  case PutHash:
    return kv.applyPut(op.Key, op.Value, true)
  }
  return "",ErrNoOp
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

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  seq := kv.px.Max()+1
  key := args.Key
  reqId := args.ReqId

  kv.log("Get init, seq=%d, key=%s, reqId=%s", seq, key, reqId)

  op := Op{Operation: Get, ReqId: reqId, Key: key}
  kv.px.Start(seq, op)

  to := 10 * time.Millisecond
  for {
    decided,result := kv.px.Status(seq)
    if decided {
      // kv.log("Get decided, seq=%d", seq)

      if result == op {
        for {
          kv.mu.Lock()
          val,ok := kv.cache[seq]
          delete(kv.cache, seq) // free memory
          kv.mu.Unlock()
          if ok {
            reply.Value = val
            reply.Err = OK

            kv.log("Get done, key=%s, val=%s", key, val)

            break
          }
        }

        break
      } else {
        seq = kv.px.Max()+1
        kv.px.Start(seq, op)
      }
    }

    kv.log("Get retry w/ seq=%d", seq)
    time.Sleep((time.Duration(rand.Int() % 100)  * time.Millisecond) + to)
    if to < 100 * time.Millisecond {
      to *= 2
    }
  }

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  seq := kv.px.Max()+1
  key := args.Key
  val := args.Value
  reqId := args.ReqId

  kv.log("Put init, seq=%d, key=%s, val=%s, reqId=%s", seq, key, val, reqId)

  var operation Operation
  if args.DoHash {
    operation = PutHash
  } else {
    operation = Put
  }

  op := Op{Operation: operation, ReqId: reqId, Key: key, Value: val}
  kv.px.Start(seq, op)

  to := 10 * time.Millisecond
  for {
    decided,result := kv.px.Status(seq)
    if decided {
      // kv.log("Put decided, seq=%d", seq)

      if result == op {

        for {
          kv.mu.Lock()
          oldval,ok := kv.cache[seq]
          kv.mu.Unlock()

          if ok {
            reply.PreviousValue = oldval
            reply.Err = OK

            kv.log("Put done, key=%s, oldval=%s", key, oldval)

            break
          }
        }

        break
      } else {
        seq = kv.px.Max()+1
        kv.px.Start(seq, op)
      }
    }
    kv.log("Put retry w/ seq=%d", seq)
    time.Sleep((time.Duration(rand.Int() % 100)  * time.Millisecond) + to)
    if to < 100 * time.Millisecond {
      to *= 2
    }
  }

  return nil
}

func (kv *KVPaxos) tick() {

  timeout := 10 * time.Millisecond

  for kv.dead == false {
    kv.mu.Lock()
    seq := kv.highestDone+1
    decided,result := kv.px.Status(seq)
    if decided {
      // apply the operation
      op,_ := result.(Op)
      val,_ := kv.applyOp(&op)
      kv.px.Done(seq)
      kv.cache[seq] = val
      kv.highestDone += 1
      kv.log("Apply %s from seq=%d", op.Operation, seq)

      // reset timeout
      timeout = 10 * time.Millisecond

      kv.mu.Unlock()
    } else {
      kv.mu.Unlock()
      // kv.log("Retry for seq=%d", seq)

      if timeout > 100 * time.Millisecond {
        kv.px.Start(seq, Op{Operation: Noop})
      }

      // wait before retrying
      time.Sleep(timeout)
      if timeout < 100 * time.Millisecond {
        // expotential backoff
        timeout *= 2
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
  kv.cache = make(map[int]string)
  kv.highestDone = -1

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

