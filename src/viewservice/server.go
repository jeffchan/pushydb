
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "os"
import "strconv"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  lastPing map[string]*LastPing
  view View // the current view
}

type LastPing struct {
  Viewnum uint // last ping's viewnum
  Timestamp time.Time // last ping's timestamp
  Restart bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  address := args.Me
  _, ok := vs.lastPing[address]

  var newPing LastPing
  newPing.Viewnum = args.Viewnum
  newPing.Timestamp = time.Now()

  log.Println(address + " ping #" + strconv.Itoa(int(args.Viewnum)))
  if ok {
    if args.Viewnum == 0 && vs.view.Viewnum > 0 {
      log.Println(" Signal restart")
      newPing.Restart = true
    } else {
      newPing.Restart = false
    }
  } else {
    newPing.Restart = false
  }
  vs.lastPing[address] = &newPing
  reply.View = vs.view

  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  reply.View = vs.view

  return nil
}

func (vs *ViewServer) isDead(address string) bool {
  if address == "" {
    return true
  }

  lastPing, ok := vs.lastPing[address]
  if ok {
    return time.Now().Sub(lastPing.Timestamp) > (DeadPings * PingInterval)
  }
  return true
}

func (vs *ViewServer) primaryEligible(address string) bool {
  lastPing, ok := vs.lastPing[address]
  if !ok {
    return false
  }

  return !(vs.isDead(address) || lastPing.Restart)
}

func (vs *ViewServer) nextAvail() string {
  for address,_ := range vs.lastPing {
    if (!vs.isDead(address) &&
        vs.view.Primary != address &&
        vs.view.Backup != address) {
      return address
    }
  }
  return ""
}

func (vs *ViewServer) nextView(primary string, secondary string) {
  vs.view = View{ vs.view.Viewnum+1, primary, secondary }
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  primary := vs.view.Primary
  backup := vs.view.Backup

  // When the viewservice first starts, it should accept
  // any server at all as the first primary
  if vs.view.Viewnum == 0 {
    avail := vs.nextAvail()
    if avail != "" {
      vs.nextView(avail, "")
    }
    return
  }

  lastPing, ok := vs.lastPing[primary]
  if !ok { return }

  // Special case if the primary restarted, must have new primary
  if vs.lastPing[primary].Restart {
    if vs.primaryEligible(backup) {
      vs.nextView(backup, vs.nextAvail())
    } else {
      // screwed if backup is not eligble -> all data lost
      vs.nextView("", "")
    }
    return
  }

  // Don't proceed to new view until the primary from current view
  // sends an ACK that it is operating in current view
  if lastPing.Viewnum < vs.view.Viewnum {
    return
  }

  if !vs.primaryEligible(primary) {
    // promote backup to primary
    vs.nextView(backup, vs.nextAvail())
  } else if vs.isDead(backup) {
    vs.nextView(primary, vs.nextAvail())
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  vs.dead = false
  vs.lastPing = make(map[string]*LastPing)
  vs.view = View{0, "", ""}

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        log.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
