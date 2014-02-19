package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format + "\n", a...)
  }
  return
}

type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}

  view *viewservice.View
  table map[string]string
  reqs map[string]string
  stateTransfer chan bool
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
  // pb.mu.Lock()
  // defer pb.mu.Unlock()
  reply.Table = pb.table
  reply.Reqs = pb.reqs

  pb.stateTransfer <- true

  return nil
}

func (pb *PBServer) PutRelay(args *PutRelayArgs, reply *PutRelayReply) error {

  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.isPrimary() {
    reply.Err = ErrWrongServer
    return nil
  }

  // if !pb.isSynced() {
  //   reply.Err = ErrOutOfSync
  //   return nil
  // }

  // val,_ := pb.table[args.Key]
  // if val != args.PreviousValue {
  //   reply.Err = ErrOutOfSync
  // } else {
    pb.table[args.Key] = args.Value
    reply.Err = OK
  // }

  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if !pb.isPrimary() {
    reply.Err = ErrWrongServer
    return nil
  }

  // filter duplicates
  dup, ok := pb.reqs[args.Id]
  if ok {
    reply.Err = ErrDupRequest
    reply.PreviousValue = dup
    return nil
  }

  old, ok := pb.table[args.Key]
  if !ok {
    old = ""
  }

  if args.DoHash {
    pb.table[args.Key] = strconv.Itoa(int(hash(old + args.Value)))
  } else {
    pb.table[args.Key] = args.Value
  }

  if pb.hasBackup() {
    // call backup (if backup exists)
    // block until we get an ok
    var relayReply PutRelayReply
    relayArgs := PutRelayArgs{args.Key, pb.table[args.Key], old}
    ok = call(pb.view.Backup, "PBServer.PutRelay", &relayArgs, &relayReply)
    if !ok || relayReply.Err != OK {
      reply.Err = relayReply.Err
      return nil
    }
  }

  reply.PreviousValue = old
  reply.Err = OK
  pb.reqs[args.Id] = old

  return nil
}

func (pb *PBServer) GetRelay(args *GetRelayArgs, reply *GetRelayReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.isPrimary() {
    reply.Err = ErrWrongServer
    return nil
  }

  // if !pb.isSynced() {
  //   reply.Err = ErrOutOfSync
  //   return nil
  // }

  val,ok := pb.table[args.Key]
  if !ok || val != args.Value {
    reply.Err = ErrOutOfSync
  } else {
    reply.Err = OK
  }

  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if !pb.isPrimary() {
    reply.Err = ErrWrongServer
    return nil
  }

  // filter duplicates
  dup, ok := pb.reqs[args.Id]
  if ok {
    reply.Err = ErrDupRequest
    reply.Value = dup
    return nil
  }

  val, ok := pb.table[args.Key]
  if !ok {
    reply.Err = ErrNoKey
    return nil
  }

  if pb.hasBackup() {
    // call backup (if backup exists)
    // block until we get an ok
    var relayReply GetRelayReply
    relayArgs := GetRelayArgs{args.Key, val}
    ok = call(pb.view.Backup, "PBServer.GetRelay", &relayArgs, &relayReply)
    if !ok || relayReply.Err != OK {
      reply.Err = relayReply.Err
      return nil
    }
  }

  reply.Value = val
  reply.Err = OK
  pb.reqs[args.Id] = val

  return nil
}

func (pb *PBServer) isPrimary() bool {
  return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
  return pb.view.Backup == pb.me
}

func (pb *PBServer) hasBackup() bool {
  return pb.isPrimary() && pb.view.Backup != ""
}

func (pb *PBServer) isSynced() bool {
  return len(pb.table) > 0
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  old := pb.view

  view,_ := pb.vs.Ping(old.Viewnum)
  pb.view = &view

  if (view.Viewnum - old.Viewnum) == 1 &&
     old.Backup != view.Backup && old.Backup != pb.me {
    if pb.isBackup() {
      var reply SyncReply
      call(view.Primary, "PBServer.Sync", &SyncArgs{}, &reply)
      pb.table = reply.Table
      pb.reqs = reply.Reqs
      // TODO check call result
    } else if pb.isPrimary() {
      <- pb.stateTransfer
    }
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  pb.view = &viewservice.View{0, "", ""}
  pb.table = make(map[string]string)
  pb.reqs = make(map[string]string)
  pb.stateTransfer = make(chan bool)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait()
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
