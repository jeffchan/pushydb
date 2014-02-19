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
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  mu sync.Mutex
  sync bool
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

func (pb *PBServer) shortAddr() string {
  name := pb.me[17:]
  if pb.isPrimary() {
    return "P " + name
  } else if pb.isBackup() {
    return "B " + name
  } else {
    return "X"
  }
}

const Log = false
func (pb *PBServer) log(format string, a ...interface{}) (n int, err error) {
  if Log {
    n, err = fmt.Printf(pb.shortAddr() + ": " + format + "\n", a...)
  }
  return
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if !pb.isPrimary() {
    pb.log("error: Sync from not primary")
    reply.Err = ErrOutOfSync
    return nil
  }

  if args.To != pb.view.Backup {
    pb.log("error: Sync from %s, expecting %s", args.To, pb.view.Backup)
    reply.Err = ErrOutOfSync
    return nil
  }

  if pb.table == nil {
    pb.log("error: NIL TABLE")
    reply.Err = ErrOutOfSync
    return nil
  }

  reply.Table = pb.table
  reply.Reqs = pb.reqs
  reply.Err = OK

  pb.sync = false

  return nil
}

func (pb *PBServer) PutRelay(args *PutRelayArgs, reply *PutRelayReply) error {

  if pb.sync {
    pb.log("error: busy syncing, reject put relay")
    reply.Err = ErrBusy
    return nil
  }

  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.isPrimary() {
    pb.log("error: put relay to primary")
    reply.Err = ErrWrongServer
    return nil
  }

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
  if pb.sync {
    pb.log("error: busy syncing, reject put")
    reply.Err = ErrBusy
    return nil
  }

  pb.mu.Lock()
  defer pb.mu.Unlock()

  pb.log("put %s (%s)", args.Key, args.Id)
  defer pb.log("put %s exit", args.Key)

  if !pb.isPrimary() {
    pb.log("error: put to backup")
    reply.Err = ErrWrongServer
    return nil
  }

  // filter duplicates
  dup, ok := pb.reqs[args.Id]
  if ok {
    pb.log("error: dup request id %s", args.Id)
    reply.Err = ErrDupRequest
    reply.PreviousValue = dup
    return nil
  }

  oldval, ok := pb.table[args.Key]
  if !ok {
    oldval = ""
  }

  var newval string
  if args.DoHash {
    newval = strconv.Itoa(int(hash(oldval + args.Value)))
  } else {
    newval = args.Value
  }

  // call backup (if backup exists)
  if pb.hasBackup() {
    pb.log("forward to backup %s", pb.view.Backup)
    var relayReply PutRelayReply
    relayArgs := PutRelayArgs{args.Key, newval, oldval}
    ok = call(pb.view.Backup, "PBServer.PutRelay", &relayArgs, &relayReply)
    if !ok || relayReply.Err != OK {
      pb.log("error: no response from putrelay")
      reply.Err = relayReply.Err
      return nil
    }
    pb.log("forward to backup %s done", pb.view.Backup)
  }

  reply.PreviousValue = oldval
  reply.Err = OK
  pb.table[args.Key] = newval
  pb.reqs[args.Id] = oldval

  pb.log("put k: %s, v: %s, prev: %s", args.Key, newval, oldval)

  return nil
}

func (pb *PBServer) GetRelay(args *GetRelayArgs, reply *GetRelayReply) error {
  if pb.sync {
    pb.log("error: busy syncing, reject get relay")
    reply.Err = ErrBusy
    return nil
  }

  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.isPrimary() {
    pb.log("error: get relay to primary")
    reply.Err = ErrWrongServer
    return nil
  }

  val,ok := pb.table[args.Key]
  if !ok {
    pb.log("error: key %s does not exist", args.Key)
    reply.Err = ErrOutOfSync
  } else if val != args.Value {
    pb.log("error: val does not match. primary: %s vs backup: %s", args.Value, val)
    reply.Err = ErrOutOfSync
  } else {
    reply.Err = OK
  }

  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  if pb.sync {
    pb.log("error: busy syncing, reject get")
    reply.Err = ErrBusy
    return nil
  }

  pb.mu.Lock()
  defer pb.mu.Unlock()

  pb.log("get %s (%s)", args.Key, args.Id)
  defer pb.log("get %s exit", args.Key)

  if !pb.isPrimary() {
    pb.log("error: get request to backup")
    reply.Err = ErrWrongServer
    return nil
  }

  // filter duplicates
  dup, ok := pb.reqs[args.Id]
  if ok {
    pb.log("error: dup request id %s", args.Id)
    reply.Err = ErrDupRequest
    reply.Value = dup
    return nil
  }

  val, ok := pb.table[args.Key]
  if !ok {
    pb.log("error: key %s does not exist", args.Key)
    reply.Err = ErrNoKey
    return nil
  }

  // call backup (if backup exists)
  if pb.hasBackup() {
    pb.log("forward to backup %s", pb.view.Backup)
    var relayReply GetRelayReply
    relayArgs := GetRelayArgs{args.Key, val}
    ok = call(pb.view.Backup, "PBServer.GetRelay", &relayArgs, &relayReply)
    if !ok || relayReply.Err != OK {
      pb.log("error: no response from getrelay")
      reply.Err = relayReply.Err
      return nil
    }
    pb.log("forward to backup %s done", pb.view.Backup)
  }

  reply.Value = val
  reply.Err = OK
  pb.reqs[args.Id] = val

  return nil
}

func (pb *PBServer) inView() bool {
  return pb.isPrimary() || pb.isBackup()
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

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  old := pb.view

  view,_ := pb.vs.Ping(old.Viewnum)
  pb.view = &view

  if !pb.inView() { return }

  if (view.Viewnum - old.Viewnum) == 1 {
    pb.log("New view #%d, P: %s, B: %s", view.Viewnum, view.Primary, view.Backup)
  }

  if (view.Viewnum - old.Viewnum) == 1 &&
    old.Backup != view.Backup && old.Backup != pb.me {

    pb.log("New backup %s online in viewnum #%d", view.Backup, view.Viewnum)

    pb.sync = true

    if pb.isBackup() {
      go func() {
        pb.mu.Lock()
        defer pb.mu.Unlock()

        var reply SyncReply
        for {
          pb.log("Syncing from %s", view.Primary)
          ok := call(view.Primary, "PBServer.Sync", &SyncArgs{pb.me}, &reply)
          if ok && reply.Err == OK {
            pb.table = reply.Table
            pb.reqs = reply.Reqs
            pb.sync = false
            pb.log("Synced to table of size %d", len(pb.table))
            break
          }
          time.Sleep(viewservice.PingInterval)
        }

      }()
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
