package messagebroker

import "net"
import "fmt"
import "net/rpc"
import "paxos"
import "sync"
import "os"
import "log"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "time"
import "reflect"

const (
  ServerLog           = false
  InitTimeout         = 10 * time.Millisecond
  ClientRetryInterval = 100 * time.Millisecond
)

type MBServer struct {
  mu         sync.Mutex
  l          net.Listener
  me         int
  dead       bool // for testing
  unreliable bool // for testing
  px         *paxos.Paxos

  lastAppliedSeq int
  reqs           map[string]Err
}

func RandMTime() time.Duration {
  return time.Duration(rand.Int()%100) * time.Millisecond
}

func (mb *MBServer) log(format string, a ...interface{}) (n int, err error) {
  if ServerLog {
    addr := "Srv#" + strconv.Itoa(mb.me) + "|"
    n, err = fmt.Printf(addr+" >> "+format+"\n", a...)
  }
  return
}

// tell the server to shut itself down.
func (mb *MBServer) Kill() {
  mb.dead = true
  mb.l.Close()
  mb.px.Kill()
}

func (mb *MBServer) Notify(args *NotifyArgs, reply *NotifyReply) error {
  pubArgs := args.PublishArgs
  key := pubArgs.Key
  val := pubArgs.Value
  reqId := pubArgs.ReqId

  mb.log("Notify receive, key=%s, val=%s, reqId=%s, list=%s", key, val, reqId, args.Subscribers)

  op := Op{
    Operation: Notify,
    Args:      *args,
    ReqId:     reqId,
  }

  err := mb.resolveOp(op)
  reply.Err = err

  mb.log("Notify return, key=%s, val=%s, reqId=%s, list=%s, err=%s", key, val, reqId, args.Subscribers, err)

  return nil
}

func (mb *MBServer) resolveOp(op Op) Err {
  seq := mb.px.Max() + 1

  mb.mu.Lock()
  dup, exists := mb.reqs[op.ReqId]
  mb.mu.Unlock()

  if exists {
    return dup
  }

  mb.px.Start(seq, op)

  to := InitTimeout
  time.Sleep(to)

  decided, val := mb.px.Status(seq)
  for !decided || !reflect.DeepEqual(val, op) {
    if (decided && !reflect.DeepEqual(val, op)) || (seq <= mb.lastAppliedSeq) {
      mb.log("Seq=%d already decided", seq)
      seq = mb.px.Max() + 1
      mb.px.Start(seq, op)
    }

    // mb.log("Retry w/ seq=%d", seq)
    time.Sleep(to + RandMTime())
    if to < 100*time.Millisecond {
      to *= 2
    }

    decided, val = mb.px.Status(seq)
  }

  mb.log("Seq=%d decided!", seq)

  // block until seq op has been applied
  for mb.lastAppliedSeq < seq {
    time.Sleep(InitTimeout)
  }

  mb.mu.Lock()
  result, exists := mb.reqs[op.ReqId]
  mb.mu.Unlock()

  mb.px.Done(seq)

  return result
}

func (mb *MBServer) applyNotify(args NotifyArgs) Err {
  for client, sub := range args.Subscribers {
    if !sub {
      continue
    }

    pubargs := &args.PublishArgs
    var reply PublishReply

    for !mb.dead {
      call_ok := call(client, "Clerk.Publish", pubargs, &reply)
      if call_ok && reply.Err == OK {
        break
      }
      time.Sleep(ClientRetryInterval)
    }
  }

  return OK
}

func (mb *MBServer) applyOp(op *Op) Err {
  // Return early for a noop
  if op.Operation == Noop {
    return ErrNoOp
  }

  // Check if operation was already applied
  mb.mu.Lock()
  _, exists := mb.reqs[op.ReqId]
  mb.mu.Unlock()

  if exists {
    return ErrAlreadyApplied
  }

  switch op.Operation {
  case Notify:
    args := op.Args.(NotifyArgs)
    return mb.applyNotify(args)
  }

  // Should not reach this point
  return ErrInvalid
}

func (mb *MBServer) logSync() {

  timeout := InitTimeout

  for mb.dead == false {

    seq := mb.lastAppliedSeq + 1
    decided, result := mb.px.Status(seq)

    if decided {
      // apply the operation
      op, _ := result.(Op)
      err := mb.applyOp(&op)

      mb.mu.Lock()
      if err == OK {
        mb.reqs[op.ReqId] = err
      }

      mb.lastAppliedSeq += 1
      mb.mu.Unlock()

      if err != ErrAlreadyApplied {
        mb.log("Applied %s of reqId=%s", op.Operation, op.ReqId)
      } else {
        mb.log("Already applied %s of reqId=%s", op.Operation, op.ReqId)
      }

      // reset timeout
      timeout = InitTimeout

    } else {
      // mb.log("Retry for seq=%d", seq)

      if timeout >= 1*time.Second {
        mb.log("Try noop for seq=%d", seq)
        mb.px.Start(seq, Op{Operation: Noop})

        // wait for noop to return
        noopDone := false
        for !noopDone {
          noopDone, _ = mb.px.Status(seq)
          time.Sleep(100 * time.Millisecond)
        }
      } else {
        // wait before retrying
        time.Sleep(timeout)

        if timeout < 1*time.Second {
          // expotential backoff
          timeout *= 2
        }
      }
    }
  }
}

func StartServer(servers []string, me int) *MBServer {
  gob.Register(Op{})
  gob.Register(NotifyArgs{})

  mb := new(MBServer)
  mb.me = me
  mb.reqs = make(map[string]Err)
  mb.lastAppliedSeq = -1

  rpcs := rpc.NewServer()
  rpcs.Register(mb)

  mb.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me])
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  mb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for mb.dead == false {
      conn, err := mb.l.Accept()
      if err == nil && mb.dead == false {
        if mb.unreliable && (rand.Int63()%1000) < 100 {
          // discard the request.
          conn.Close()
        } else if mb.unreliable && (rand.Int63()%1000) < 200 {
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
      if err != nil && mb.dead == false {
        fmt.Printf("MBServer(%v) accept: %v\n", me, err.Error())
        mb.Kill()
      }
    }
  }()

  go mb.logSync()

  return mb
}