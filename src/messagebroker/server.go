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
  rpc        sync.Mutex
  l          net.Listener
  me         int
  dead       bool // for testing
  unreliable bool // for testing
  px         *paxos.Paxos

  lastAppliedSeq int
  reqs           map[string]map[int64]Err          // Key -> Ver -> Err
  buffer         map[string]map[int64]*PublishArgs // Key -> Ver -> PublishArgs
  next           map[string]int64                  // Key -> Ver
  subscribers    map[string]*Subscriber            // Client -> Subscribe struct
}

type Subscriber struct {
  mu       sync.Mutex
  Next     map[string]int64
  QuitChan chan bool
}

func (s *Subscriber) Subscribe(key string, start int64) bool {
  s.mu.Lock()
  defer s.mu.Unlock()

  first := false

  _, ok := s.Next[key]
  // check if not already subscribed
  if !ok {
    s.Next[key] = start

    first = len(s.Next) == 1
  }

  return first
}

func (s *Subscriber) Unsubscribe(key string) bool {
  s.mu.Lock()
  defer s.mu.Unlock()

  last := false

  _, ok := s.Next[key]
  // check if actually subscribed
  if ok {
    delete(s.Next, key)

    last = len(s.Next) == 0
  }

  return last
}

func NewSubscriber() *Subscriber {
  return &Subscriber{
    Next:     make(map[string]int64),
    QuitChan: make(chan bool),
  }
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

func (mb *MBServer) NotifyPut(args *NotifyPutArgs, reply *NotifyPutReply) error {
  mb.rpc.Lock()
  defer mb.rpc.Unlock()

  key := args.Key
  version := args.Version

  if version > mb.getNext(key) {
    reply.Err = ErrOutOfOrder
    return nil
  }

  mb.log("Notify put receive, key=%s, version=%d,", key, version)

  op := Op{
    Operation: NotifyPut,
    Args:      *args,
    Key:       key,
    Version:   version,
  }

  err := mb.resolveOp(op)
  reply.Err = err

  mb.log("Notify put return, key=%s, version=%d, err=%s", key, version, err)

  return nil
}

func (mb *MBServer) NotifySubscribe(args *NotifySubscribeArgs, reply *NotifySubscribeReply) error {
  mb.rpc.Lock()
  defer mb.rpc.Unlock()

  key := args.Key
  version := args.Version

  if version > mb.getNext(key) {
    reply.Err = ErrOutOfOrder
    return nil
  }

  mb.log("Notify subscribe receive, key=%s, version=%d,", key, version)

  op := Op{
    Operation: NotifySubscribe,
    Args:      *args,
    Key:       key,
    Version:   version,
  }

  err := mb.resolveOp(op)
  reply.Err = err

  mb.log("Notify subscribe return, key=%s, version=%d, err=%s", key, version, err)

  return nil
}

func (mb *MBServer) resolveOp(op Op) Err {
  seq := mb.px.Max() + 1

  dup, exists := mb.getReqs(op.Key, op.Version)
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

  err, exists := mb.getReqs(op.Key, op.Version)

  mb.px.Done(seq)

  return err
}

func (mb *MBServer) applyNotifyPut(args NotifyPutArgs) Err {
  key := args.Key
  version := args.Version

  // We may receive these out of order
  // Cache the notification
  publishArgs := &PublishArgs{
    Type:    Put,
    ReqId:   args.ReqId,
    PutArgs: args,
  }
  mb.setBuffer(key, version, publishArgs)

  mb.setNext(key, version+1)

  return OK
}

func (mb *MBServer) applyNotifySubscribe(args NotifySubscribeArgs) Err {
  key := args.Key
  version := args.Version

  // We may receive these out of order
  // Cache the notification
  publishArgs := &PublishArgs{
    Type:          Subscribe,
    ReqId:         args.ReqId,
    SubscribeArgs: args,
  }
  mb.setBuffer(key, version, publishArgs)

  addr := args.Address
  unsub := args.Unsubscribe

  s, ok := mb.subscribers[addr]
  if !ok {
    s = NewSubscriber()

    mb.mu.Lock()
    mb.subscribers[addr] = s
    mb.mu.Unlock()
  }

  if !unsub {
    start := s.Subscribe(key, args.Version)
    if start {
      go mb.publish(addr, s, s.QuitChan)
    }
  } else {
    done := false
    for !done {
      done = s.Next[key] == version+1
      time.Sleep(50 * time.Millisecond)
    }
    end := s.Unsubscribe(key)
    if end {
      s.QuitChan <- true
    }
  }

  mb.setNext(key, version+1)

  return OK
}

func (mb *MBServer) publish(addr string, s *Subscriber, quit chan bool) {
  dummy := make(chan bool)
  go func() { dummy <- true }()
  for !mb.dead {
    select {
    case <-dummy:
      for key, next := range s.Next {
        args, exists := mb.getBuffer(key, next)
        if !exists {
          continue
        }

        // Ignore other client's sub/unsub
        if args.Type == Subscribe && args.Addr() != addr {
          s.Next[key] = next + 1
          continue
        }

        var reply PublishReply
        ok := call(addr, "Clerk.Publish", args, &reply)
        if ok && reply.Err == OK {
          s.Next[key] = next + 1
        }
      }
      time.Sleep(50 * time.Millisecond)
      go func() { dummy <- true }()
    case <-quit:
      return
    }
  }
}

func (mb *MBServer) applyOp(op *Op) Err {
  // Return early for a noop
  if op.Operation == Noop {
    return ErrNoOp
  }

  // Check if operation was already applied
  _, exists := mb.getReqs(op.Key, op.Version)

  if exists {
    return ErrAlreadyApplied
  }

  switch op.Operation {
  case NotifySubscribe:
    args := op.Args.(NotifySubscribeArgs)
    return mb.applyNotifySubscribe(args)
  case NotifyPut:
    args := op.Args.(NotifyPutArgs)
    return mb.applyNotifyPut(args)
  }

  // Should not reach this point
  return ErrInvalid
}

func (mb *MBServer) getNext(key string) int64 {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  next, exists := mb.next[key]
  if exists {
    return next
  }

  return 1
}

func (mb *MBServer) setNext(key string, ver int64) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  mb.next[key] = ver
}

func (mb *MBServer) getReqs(key string, ver int64) (Err, bool) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  reqs, exists := mb.reqs[key]
  if !exists {
    mb.reqs[key] = make(map[int64]Err)
    return "", false
  }

  result, ok := reqs[ver]

  return result, ok
}

func (mb *MBServer) setBuffer(key string, ver int64, args *PublishArgs) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  _, exists := mb.buffer[key]
  if !exists {
    mb.buffer[key] = make(map[int64]*PublishArgs)
  }
  mb.buffer[key][ver] = args
}

func (mb *MBServer) getBuffer(key string, ver int64) (*PublishArgs, bool) {
  mb.mu.Lock()
  defer mb.mu.Unlock()

  buffer, exists := mb.buffer[key]
  if !exists {
    mb.buffer[key] = make(map[int64]*PublishArgs)
    return nil, false
  }

  result, ok := buffer[ver]
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
        mb.reqs[op.Key][op.Version] = err
      }

      mb.lastAppliedSeq += 1
      mb.mu.Unlock()

      if err != ErrAlreadyApplied {
        // mb.log("Applied %s of Notify.Version=%d", op.Operation, op.Version)
      } else {
        // mb.log("Already applied %s of Notify.Version=%d", op.Operation, op.Version)
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
  gob.Register(NotifyPutArgs{})
  gob.Register(NotifySubscribeArgs{})

  mb := new(MBServer)
  mb.me = me
  mb.reqs = make(map[string]map[int64]Err)
  mb.buffer = make(map[string]map[int64]*PublishArgs)
  mb.next = make(map[string]int64)
  mb.subscribers = make(map[string]*Subscriber)
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
