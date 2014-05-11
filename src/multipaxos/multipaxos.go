package multipaxos

//
// MultiPaxos library, to be included in an application.
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
import "strconv"
import "time"

const (
  Debug = 0
  OK = "OK"
  MultiPaxosOn = true
)

type Err string

func DPrintf(a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Println(a...)
  }
  return
}

type Paxos struct {
  mu sync.Mutex
  mu2 sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  Seqs           map[int]*Agreement
  HighestDones   map[string]int // one for every peer
  Minimum        int
  iAmLeader      bool
  peerTracker    []int
  prepareYourself bool
}

type Agreement struct {
  Number int64
  Value interface{}
  HighestPrepare int64
  HighestAcceptNumber int64
  HighestAcceptValue interface{}
  Decided bool
  PrepareChan chan PrepareReply
  AcceptChan chan AcceptReply
  ProposerRunning bool
}

type HighestDone struct {
  Count int
  Me string
}

type PrepareArgs struct {
  Seq int
  Number int64
  HD HighestDone
}

type PrepareReply struct {
  Seq int
  OK bool
  Number int64
  Value interface{}
  HD HighestDone
}

type AcceptArgs struct {
  Seq int
  Number int64
  Value interface{}
  HD HighestDone
}

type AcceptReply struct {
  Seq int
  OK bool
  Number int64
  HD HighestDone
  Value interface{}
}

type DecideArgs struct {
  Seq int
  Value interface{}
  HD HighestDone
}

type DecideReply struct {
  Seq int
  OK bool
}

type PingArgs struct {
  ID int // index into peers
}
type PingReply struct {
  Err Err
  HighestDone int
}

type StartArgs struct {
  Seq int // sequence number
  V   interface{}
}
type StartReply struct {
  Err Err
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

  // fmt.Println(err)
  return false
}

func (px *Paxos) Proposer(seq int, vin interface{}) { // at most one proposer per sequence per machine
  it := px.getSeq(seq)

  if it.ProposerRunning {
    return
  } else {
    it.ProposerRunning = true
  }

  for !px.dead && !it.Decided {
    retry := false
    // choose n, unique and higher than any n seen so far
    n := time.Now().Unix() * int64(len(px.peers)) + int64(px.me)
    var v_prime interface{}

    if (px.prepareYourself && px.iAmLeader) || !px.iAmLeader {
      // send prepare(n) to all servers including self
      prepareReplies := make(map[int]PrepareReply)
      i := 0
      for _,peer := range px.peers{
        px.DoPrepare(seq, peer, n, prepareReplies, i)
        i++
      }

      // count number of successful prepares
      count := 0
      for _, r := range prepareReplies{
        if r.Number == -2{
        } else if r.Number == -1 {
          // update highestdones
          px.HighestDones[r.HD.Me] = r.HD.Count

          it.Decided = true
          it.Value = r.Value
          return
        } else {
          // update highestdones
          px.HighestDones[r.HD.Me] = r.HD.Count

          if r.OK {
            // successful prepare ack
            count++
          } else {
            retry = true
            if r.Number != 0 {
              it.HighestPrepare = r.Number
            }
            break
          }
        }
      }
      if count > (len(px.peers) - 1)/2 {
        // if prepare_ok(n_a, v_a) from majority
      } else {
        // not majority
        retry = true
      }

      if retry {
        // sleep for a random amount of time (this should randomly elect a proposer leader)
        time.Sleep(time.Duration(rand.Int() % 1000) * time.Millisecond)
        continue
      }

      px.mu.Lock()
      // v' = v_a with highest n_a; choose own v otherwise
      var max_n int64
      max_n = 0
      for _, r := range prepareReplies {
        if r.Number > max_n && r.Value != nil{
          v_prime = r.Value
          max_n = r.Number
        }
      }
      if max_n == 0 {
        v_prime = vin
      }
      px.mu.Unlock()

      px.prepareYourself = false
    } else {
      // fmt.Printf("Skipping prepare phase because I'm the established leader\n")
    }

    if v_prime == nil {
      v_prime = vin
    }
    // send accept(n, v') to all
    acceptReplies := make(map[int]AcceptReply)
    i := 0
    for _, peer := range px.peers {
      px.DoAccept(seq, peer, n, v_prime, acceptReplies, i)
      i++
    }

    // count number of successful accepts
    count := 0
    for _, r := range acceptReplies {
      // update highestdones
      px.HighestDones[r.HD.Me] = r.HD.Count

      if r.Number == -1 {
        it.Decided = true
        it.Value = r.Value
        return
      } else if r.Number == -2 {
        // ignore
      } else {
        if r.OK {
          // valid accept ack
          count++
        } else {
          retry = true
          it.HighestPrepare = r.Number
          break
        }
      }
    }
    if count > (len(px.peers) - 1)/2 {
      // if accept_ok(n) from majority
    } else {
      // not majority
      retry = true
    }

    if retry {
      // sleep for a random amount of time (this should randomly elect a proposer leader)
      time.Sleep(time.Duration(rand.Int() % 1000) * time.Millisecond)
      continue
    }

    // send decided(v') to all
    for _,peer := range px.peers {
      go px.DoDecide(seq, peer, v_prime)
    }
    // just let the other paxos instances figure out the final value if these don't go through

    return
  }
}

func (px *Paxos) DoPrepare(seq int, peer string, n int64, replies map[int]PrepareReply, ind int) {
  args := PrepareArgs{seq, n, HighestDone{px.HighestDones[px.peers[px.me]], px.peers[px.me]}}
  var prepareReply PrepareReply
  if px.peers[px.me] == peer {
    px.Prepare(args, &prepareReply)
  } else {
    ok := call(peer, "Paxos.Prepare", args, &prepareReply)
    if !ok {
      prepareReply.OK = false
      prepareReply.Number = -2
    }
  }
  replies[ind] = prepareReply
}

func (px *Paxos) DoAccept(seq int, peer string, n int64, v_prime interface{}, replies map[int]AcceptReply, ind int) {
  args := AcceptArgs{seq, n, v_prime, HighestDone{px.HighestDones[px.peers[px.me]], px.peers[px.me]}}
  var acceptReply AcceptReply
  if px.peers[px.me] == peer {
    px.Accept(args, &acceptReply)
  } else {
    ok := call(peer, "Paxos.Accept", args, &acceptReply)
    if !ok {
      acceptReply.OK = false
      acceptReply.Number = -2
    }
  }
  replies[ind] = acceptReply
}

func (px *Paxos) DoDecide(seq int, peer string, v_prime interface{}) {
  args := DecideArgs{seq, v_prime, HighestDone{px.HighestDones[px.peers[px.me]], px.peers[px.me]}}
  var decideReply DecideReply
  if px.peers[px.me] == peer {
    px.Decide(args, &decideReply)
  } else {
    call(peer, "Paxos.Decide", args, &decideReply)
  }
}

func (px *Paxos) Prepare(args PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  // transfer highestdones info
  px.HighestDones[args.HD.Me] = args.HD.Count
  reply.HD = HighestDone{px.HighestDones[px.peers[px.me]], px.peers[px.me]}

  seq := args.Seq
  n := args.Number
  it := px.getSeq(seq)

  if it.Decided {
    reply.OK = false
    reply.Number = -1
    reply.Value = it.Value
    return nil
  }

  if n > it.HighestPrepare {
    it.HighestPrepare = n
    reply.OK = true
    reply.Number = it.HighestAcceptNumber
    reply.Value = it.HighestAcceptValue
  } else {
    reply.OK = false
    reply.Number = it.HighestPrepare
  }
  return nil
}

func (px *Paxos) Accept(args AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  // transfer highestdones info
  px.HighestDones[args.HD.Me] = args.HD.Count
  reply.HD = HighestDone{px.HighestDones[px.peers[px.me]], px.peers[px.me]}

  n := args.Number
  seq := args.Seq
  v := args.Value
  it := px.getSeq(seq)

  if it.Decided {
    reply.OK = false
    reply.Number = -1
    reply.Value = it.Value
    return nil
  }

  if n >= it.HighestPrepare {
    it.HighestPrepare = n
    it.HighestAcceptNumber = n
    it.HighestAcceptValue = v
    reply.OK = true
    reply.Number = it.HighestAcceptNumber
  } else {
    reply.OK = false
    reply.Number = it.HighestPrepare
  }
  return nil
}

func (px *Paxos) Decide(args DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  // transfer highestdones info
  px.HighestDones[args.HD.Me] = args.HD.Count

  seq := args.Seq
  it := px.getSeq(seq)

  it.Decided = true
  it.Value = args.Value
  reply.Seq = args.Seq
  reply.OK = true
  return nil
}

func (px *Paxos) GarbageMan() {
  for px.dead == false{
    for i, _ := range px.Seqs {
      if i < px.Minimum {
        delete(px.Seqs, i)
      }
    }
    time.Sleep(50 * time.Millisecond)
  }
}

// tries to clean up any remaining undecided instances by starting proposer on it
func (px *Paxos) Pusher() {
  for px.dead == false {
    px.mu.Lock()
    for i, s := range px.Seqs {
      if !s.Decided{
        DPrintf("[PAXOS] " + px.peers[px.me] + ": PUSHER: pushing seq " + strconv.Itoa(i))
        go px.Proposer(i, s.Value)
      }
    }
    px.mu.Unlock()
    time.Sleep(100 * time.Millisecond)
  }
}

//
// Ping other servers to tell you that you're alive
//
//
func (px *Paxos) Tick() {
  for !px.dead {
    // increment everything
    for i, _ := range px.peerTracker {
      px.peerTracker[i] += 1
    }
    // if any people above me recently pinged they are leader, otherwise I am
    check := true
    for i := px.me + 1; i < len(px.peerTracker); i++ {
      if px.peerTracker[i] < 2 {
        check = false
      }
    }
    old := px.iAmLeader
    if check {
      px.iAmLeader = true
    } else {
      px.iAmLeader = false
    }
    if px.iAmLeader && !old {
      px.prepareYourself = true
    }
    // ping everybody
    for i, srv := range px.peers {
      args := PingArgs{px.me}
      var reply PingReply
      if i == px.me {
        px.Ping(&args, &reply)
        px.HighestDones[srv] = reply.HighestDone
      } else {
        ok := call(srv, "Paxos.Ping", &args, &reply)
        if ok {
          px.HighestDones[srv] = reply.HighestDone
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (px *Paxos) Ping(args *PingArgs, reply *PingReply) error {
  px.peerTracker[args.ID] = 0
  reply.Err = OK
  reply.HighestDone = px.HighestDones[px.peers[px.me]] // piggyback
  return nil
}

func (px *Paxos) GetLeader() int {
  leader := px.me
  for i := px.me + 1; i < len(px.peerTracker); i++ {
    if px.peerTracker[i] < 2 {
      leader = i
    }
  }

  return leader
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  if seq < px.Min() {
    return
  }
  if MultiPaxosOn {
    leader := px.GetLeader()
    if leader == px.me {
      go px.Proposer(seq, v)
    } else {
      args := StartArgs{seq, v}
      var reply StartReply
      ok := call(px.peers[leader], "Paxos.Startpls", &args, &reply)
      if !ok {
        go px.Proposer(seq, v)
      }
    }
  } else {
    go px.Proposer(seq, v)
  }
}

func (px *Paxos) Startpls(args *StartArgs, reply *StartReply) error {
  if args.Seq < px.Min() {
    reply.Err = OK
    return nil
  }

  go px.Proposer(args.Seq, args.V)
  reply.Err = OK
  return nil
}

func (px *Paxos) getSeq(seq int) *Agreement {
  px.mu2.Lock()
  defer px.mu2.Unlock()

  _, ok := px.Seqs[seq]
  if !ok {
    px.Seqs[seq] = px.MakeSeq(seq)
  }
  return px.Seqs[seq]
}

func (px *Paxos) MakeSeq(seq int) *Agreement {
  ag := &Agreement{Number: 0}
  ag.PrepareChan = make(chan PrepareReply)
  ag.AcceptChan = make(chan AcceptReply)
  return ag
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  DPrintf("[PAXOS] " + px.peers[px.me] + ": DONE called on sequence" + strconv.Itoa(seq))
  if seq > px.HighestDones[px.peers[px.me]] {
    DPrintf("[PAXOS] " + px.peers[px.me] + ": DONE for seq# " + strconv.Itoa(seq) + " highest seen, now setting.")
    px.HighestDones[px.peers[px.me]] = seq
    DPrintf("[PAXOS] " + px.peers[px.me] + ": Highestdones is now:")
    DPrintf(px.HighestDones)
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  max := -1
  for k, _ := range px.Seqs {
    if k > max {
      max = k
    }
  }
  return max
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
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  min := 10000
  for _, v := range px.HighestDones {
    if v < min {
      min = v
    }
  }
  px.Minimum = min + 1
  DPrintf("[PAXOS] " + px.peers[px.me] + ": MIN() called, new minimum is: " + strconv.Itoa(px.Minimum))

  for i, _ := range px.Seqs {
    if i < px.Minimum {
      DPrintf("[PAXOS] " + px.peers[px.me] + ": Freeing memory, deleting seq: " + strconv.Itoa(i))
      delete(px.Seqs, i)
      DPrintf("[PAXOS] " + px.peers[px.me] + ": Seqs is now: ")
      DPrintf(px.Seqs)
    }
  }
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  if seq < px.Min() {
    return false, nil
  }
  DPrintf("[PAXOS] " + px.peers[px.me] + ": STATUS called for sequence :" + strconv.Itoa(seq))
  if _, ok := px.Seqs[seq]; ok {
    return px.Seqs[seq].Decided, px.Seqs[seq].Value
  } else {
    return false, "hole"
  }
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
  px.peerTracker = make([]int, len(peers))

  // Your initialization code here.
  px.Seqs = make(map[int]*Agreement)
  px.HighestDones = make(map[string]int)
  for _, v := range px.peers {
    px.HighestDones[v] = -1
  }

  // go px.GarbageMan()
  if MultiPaxosOn {
    go px.Tick()
  }
  go px.Pusher()

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
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
