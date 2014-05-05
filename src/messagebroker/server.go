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
  reqs           map[int64]map[int]Err
  notifications  map[int64]map[int]*NotifyArgs // NotifyArgs.Seq -> Results
  next           map[int64]map[string]int      // Client address -> next to publish
}

func RandMTime() time.Duration {
  return time.Duration(rand.Int()%100) * time.Millisecond
}

func (mb *MBServer) log(format string, a ...interface{}) (n int, err error) {
  if ServerLog {
    addr := "MBServer#" + strconv.Itoa(mb.me) + "|"
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
  seq := args.Seq
  pubArgs := args.PublishArgs
  key := pubArgs.Key
  val := pubArgs.Value

  mb.log("Notify receive, key=%s, val=%s, seq=%d, list=%s", key, val, seq, args.Subscribers)

  op := Op{
    Operation: Notify,
    Args:      *args,
    Seq:       seq,
    GID:       args.GID,
  }

  err := mb.resolveOp(op)
  reply.Err = err

  mb.log("Notify return, key=%s, val=%s, seq=%d, list=%s, err=%s", key, val, seq, args.Subscribers, err)

  return nil
}

func (mb *MBServer) resolveOp(op Op) Err {
  seq := mb.px.Max() + 1

  dup, exists := mb.getReqs(op.GID, op.Seq)
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

  err, exists := mb.getReqs(op.GID, op.Seq)

  mb.px.Done(seq)

  return err
}

// Goroutine per client
// Publishes notification in sequence order
func (mb *MBServer) publish(client string, gid int64, next int) {
  for !mb.dead {
    notification, exists := mb.getNotifications(gid, next)
    // Wait until next notification is available
    for !exists && !mb.dead {
      notification, exists = mb.getNotifications(gid, next)
      time.Sleep(50 * time.Millisecond)
    }

    if notification == nil {
      return
    }

    // Only publish if client on the subscriber list
    _, toPublish := notification.Subscribers[client]
    if toPublish {
      pubArgs := notification.PublishArgs
      var reply PublishReply

      // Try to publish the notification
      for !mb.dead {
        ok := call(client, "Clerk.Publish", pubArgs, &reply)
        if ok && reply.Err == OK {
          break
        }
        time.Sleep(ClientRetryInterval)
      }
    }

    // Move onto next notification
    next += 1

    mb.mu.Lock()
    mb.next[gid][client] = next
    mb.mu.Unlock()
  }
}

func (mb *MBServer) applyNotify(args NotifyArgs) Err {
  gid := args.GID
  seq := args.Seq

  // We may receive these out of order
  // Cache the notification
  mb.setNotifications(gid, seq, &args)

  for client, sub := range args.Subscribers {
    if !sub {
      continue
    }

    // Launch publish goroutine per client
    _, exists := mb.getNext(gid, client)
    if !exists {
      mb.next[gid][client] = seq
      go mb.publish(client, gid, seq)
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
  _, exists := mb.getReqs(op.GID, op.Seq)

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

func (mb *MBServer) getReqs(gid int64, seq int) (Err, bool) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  reqs, exists := mb.reqs[gid]
  if !exists {
    mb.reqs[gid] = make(map[int]Err)
    return "", false
  }

  result, ok := reqs[seq]

  return result, ok
}

func (mb *MBServer) setNotifications(gid int64, seq int, notification *NotifyArgs) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  _, exists := mb.notifications[gid]
  if !exists {
    mb.notifications[gid] = make(map[int]*NotifyArgs)
  }
  mb.notifications[gid][seq] = notification
}

func (mb *MBServer) getNotifications(gid int64, seq int) (*NotifyArgs, bool) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  notifications, exists := mb.notifications[gid]
  if !exists {
    mb.notifications[gid] = make(map[int]*NotifyArgs)
    return nil, false
  }

  result, ok := notifications[seq]
  return result, ok
}

func (mb *MBServer) getNext(gid int64, client string) (int, bool) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  next, exists := mb.next[gid]
  if !exists {
    mb.next[gid] = make(map[string]int)
    return 0, false
  }

  result, ok := next[client]
  return result, ok
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
        mb.reqs[op.GID][op.Seq] = err
      }

      mb.lastAppliedSeq += 1
      mb.mu.Unlock()

      if err != ErrAlreadyApplied {
        mb.log("Applied %s of Notify.Seq=%d", op.Operation, op.Seq)
      } else {
        mb.log("Already applied %s of Notify.Seq=%d", op.Operation, op.Seq)
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
  gob.Register(PublishArgs{})
  gob.Register(NotifyArgs{})

  mb := new(MBServer)
  mb.me = me
  mb.reqs = make(map[int64]map[int]Err)
  mb.notifications = make(map[int64]map[int]*NotifyArgs)
  mb.next = make(map[int64]map[string]int)
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
