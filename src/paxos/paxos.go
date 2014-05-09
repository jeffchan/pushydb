package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "math"
import "pdb"

const (
  Log = false
)

type Paxos struct {
  mu         sync.Mutex
  l          net.Listener
  dead       bool
  unreliable bool
  rpcCount   int
  peers      []string
  me         int // index into peers[]
  addr       string
  majority   int

  log            map[int]*Instance
  highestDone    int
  highestDoneAll int
  pdb PDB
}

type Instance struct {
  seq        int
  decidedVal interface{}

  // Acceptor's state
  prepareN    int64
  acceptedN   int64
  acceptedVal interface{}
}

func sleepRand() {
  time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)
}

func (px *Paxos) n() int64 {
  return time.Now().UnixNano()*int64(len(px.peers)) + int64(px.me)
}

func (px *Paxos) x(format string, a ...interface{}) (n int, err error) {
  if Log {
    n, err = fmt.Printf(px.shortAddr()+": "+format+"\n", a...)
  }
  return
}

func (px *Paxos) shortAddr() string {
  name := px.addr[17:]
  return name
}

func (px *Paxos) Propose(seq int, val interface{}) {
  it := px.getInstance(seq)
  for it.decidedVal == nil && !px.dead {
    n := px.n()

    // Send prepare(n)
    var maxN int64
    var maxVal interface{}
    prepareCount := 0
    for _, srv := range px.peers {
      if prepareCount >= px.majority {
        break
      }
      args := PrepareArgs{seq, n}
      var reply PrepareReply
      reply.Seq = seq
      ok := px.call(srv, "Prepare", &args, &reply)
      if ok && reply.Err == OK {
        if reply.N >= maxN {
          maxN = reply.N
          maxVal = reply.Val
        }
        prepareCount++
      }
    }

    // Try again if no majority prepare_ok
    if prepareCount < px.majority {
      sleepRand()
      continue
    }

    // Set value if none given
    if maxVal == nil {
      maxVal = val
    }

    // Send accept(n, v')
    acceptCount := 0
    for _, srv := range px.peers {
      args := AcceptArgs{seq, n, maxVal}
      var reply AcceptReply
      reply.Seq = seq
      ok := px.call(srv, "Accept", &args, &reply)
      if ok && reply.Err == OK {
        acceptCount++
      }
      if acceptCount >= px.majority {
        break
      }
    }

    // Try again if no majority accept_ok
    if acceptCount < px.majority {
      sleepRand()
      continue
    }

    // Send decided(v')
    min := math.MaxInt32
    allCount := 0
    for _, srv := range px.peers {
      args := DecidedArgs{seq, maxVal}
      var reply DecidedReply
      reply.Seq = seq
      ok := px.call(srv, "Decided", &args, &reply)
      if ok && reply.HighestDoneSeq != -1 {
        if reply.HighestDoneSeq < min {
          min = reply.HighestDoneSeq
        }
        allCount++
      }
    }

    // Free memory if common highest done seq consensus
    if allCount == len(px.peers) {
      px.free(min)
    }
  }
}

// Acceptor's prepare handler
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  it := px.getInstance(args.Seq)
  if args.N > it.prepareN {
    it.prepareN = args.N
    reply.Err = OK
    reply.HighestDoneSeq = px.highestDone // piggyback
    if it.acceptedN != -1 {
      reply.N = it.acceptedN
      reply.Val = it.acceptedVal
    }
  } else {
    reply.Err = Reject
  }
  return nil
}

// Acceptor's accept handler
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  it := px.getInstance(args.Seq)
  if args.N >= it.prepareN {
    it.prepareN = args.N
    it.acceptedN = args.N
    it.acceptedVal = args.Val

    reply.Err = OK
    reply.N = args.N
    reply.HighestDoneSeq = px.highestDone // piggyback
  } else {
    reply.Err = Reject
  }
  return nil
}

// Acceptor's decided handler
func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  it := px.getInstance(args.Seq)
  it.decidedVal = args.Val

  reply.Err = OK
  reply.HighestDoneSeq = px.highestDone // piggyback
  return nil
}

// Paxos wrapper for RPC calls - intercepts any calls to self
// by manually converting to local function call
func (px *Paxos) call(srv string, name string, args interface{}, reply interface{}) bool {
  if srv == px.addr {
    if name == "Prepare" {
      px.Prepare(args.(*PrepareArgs), reply.(*PrepareReply))
    } else if name == "Accept" {
      px.Accept(args.(*AcceptArgs), reply.(*AcceptReply))
    } else if name == "Decided" {
      px.Decided(args.(*DecidedArgs), reply.(*DecidedReply))
    } else {
      return false
    }
    return true
  } else {
    return call(srv, "Paxos."+name, args, reply)
  }
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func MakeInstance(seq int) *Instance {
  it := new(Instance)
  it.seq = seq

  // defaults
  it.decidedVal = nil

  it.prepareN = -1
  it.acceptedN = -1
  it.acceptedVal = nil

  return it
}

func (px *Paxos) getInstance(seq int) *Instance {
  px.mu.Lock()
  defer px.mu.Unlock()

  _, ok := px.log[seq]
  if !ok {
    px.log[seq] = MakeInstance(seq)
  }
  return px.log[seq]
}

func (px *Paxos) free(min int) {
  px.mu.Lock()
  defer px.mu.Unlock()

  px.highestDoneAll = min
  for key, _ := range px.log {
    if key <= min {
      delete(px.log, key)
    }
  }
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // If Start() is called with a sequence number less than Min(),
  // the Start() call should be ignored.
  if seq < px.Min() {
    return
  }

  go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  if seq > px.highestDone {
    px.highestDone = seq
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()

  maxKey := -1
  for key, _ := range px.log {
    if key > maxKey {
      maxKey = key
    }
  }
  return maxKey
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  return px.highestDoneAll + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // If Status() is called with a sequence number less than Min(),
  // Status() should return false (indicating no agreement).
  if seq < px.Min() {
    return false, nil
  }

  if seq > px.Max() {
    return false, nil
  }

  it := px.getInstance(seq)
  if it.decidedVal == nil {
    return false, nil
  }
  return true, it.decidedVal
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.addr = peers[me]
  px.majority = (len(peers) / 2) + 1
  px.highestDone = -1
  px.highestDoneAll = -1

  ///////////////////////////////////
  // start the PDB
  ///////////////////////////////////
  px.pdb = pdb.Make()

  px.log = make(map[int]*Instance)

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me])
    if e != nil {
      log.Fatal("listen error: ", e)
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63()%1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63()%1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }

  return px
}
