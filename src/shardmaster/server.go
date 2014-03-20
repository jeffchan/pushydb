package shardmaster

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
import "sort"

const (
  Debug = false
  InitTimeout = 10 * time.Millisecond
)

type ShardAlloc struct {
  GID int64
  NumShards int
  Shards []int
}

// implements sort.Interface for []ShardAlloc
type ByNumShardsInc []ShardAlloc
func (a ByNumShardsInc) Len() int           { return len(a) }
func (a ByNumShardsInc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByNumShardsInc) Less(i, j int) bool { return a[i].NumShards < a[j].NumShards }

type ByNumShardsDec []ShardAlloc
func (a ByNumShardsDec) Len() int           { return len(a) }
func (a ByNumShardsDec) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByNumShardsDec) Less(i, j int) bool { return a[i].NumShards > a[j].NumShards }

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  lastAppliedSeq int
}

const (
  OK = "OK"
  ErrNoop = "ErrNoop"
  ErrInvalid = "ErrInvalid"
  ErrGIDAlreadyJoined = "ErrGIDAlreadyJoined"
  ErrGIDAlreadyLeft = "ErrGIDAlreadyLeft"
)
type Err string

const (
  Join = "Join"
  Leave = "Leave"
  Move = "Move"
  Query = "Query"
  Noop = "Noop"
)
type Operation string

type Op struct {
  Operation Operation
  Args interface{}
}

func OpEquals(a Op, b Op) bool {
  if a.Operation != b.Operation {
    return false
  }

  if a.Operation == Join {
    aArgs := a.Args.(JoinArgs)
    bArgs := b.Args.(JoinArgs)

    if len(aArgs.Servers) != len(bArgs.Servers) {
      return false
    }

    for index,val := range aArgs.Servers {
      if val != bArgs.Servers[index] {
        return false
      }
    }

    return aArgs.GID == bArgs.GID
  }

  return a.Args == b.Args
}

func RandMTime() time.Duration {
  return time.Duration(rand.Int() % 100)  * time.Millisecond
}

func MakeConfig(num int, shards [NShards]int64, groups map[int64][]string) *Config {
  config := &Config{}
  config.Num = num
  config.Shards = shards
  config.Groups = groups
  return config
}

func CopyGroups(src map[int64][]string) map[int64][]string {
  dst := make(map[int64][]string)
  for k,v := range src {
    dst[k] = v
  }
  return dst
}

func CopyShards(src [NShards]int64) [NShards]int64 {
  var dst [NShards]int64
  for shard,gid := range src {
    dst[shard] = gid
  }
  return dst
}

func MakeShardAllocs(groups map[int64][]string, shards [NShards]int64) map[int64]*ShardAlloc {
  availGids := make(map[int64]*ShardAlloc)
  for gid,_ := range groups {
    availGids[gid] = &ShardAlloc{GID: gid, NumShards: 0, Shards: []int{}}
  }

  for shard,gid := range shards {
    alloc,exists := availGids[gid]
    // Invariant: gid should always exist in shard
    if exists {
      alloc.NumShards += 1
      alloc.Shards = append(alloc.Shards, shard)
    }
  }

  return availGids
}

func SortShardAllocs(availGids map[int64]*ShardAlloc, order string) []ShardAlloc {
  var shardAllocs []ShardAlloc
  for _,v := range availGids {
    shardAllocs = append(shardAllocs, *v)
  }

  if order == "dec" {
    sort.Sort(ByNumShardsDec(shardAllocs))
  } else {
    sort.Sort(ByNumShardsInc(shardAllocs))
  }

  return shardAllocs
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.log("Join RPC: gid=%d, servers=%s", args.GID, args.Servers)
  op := Op{Operation: Join, Args: *args}
  sm.resolveOp(op)
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.log("Leave RPC: gid=%d", args.GID)
  op := Op{Operation: Leave, Args: *args}
  sm.resolveOp(op)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.log("Move RPC: shard=%d, to_gid=%d", args.Shard, args.GID)
  op := Op{Operation: Move, Args: *args}
  sm.resolveOp(op)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.log("Query RPC: config_num=%d", args.Num)
  op := Op{Operation: Query, Args: *args}
  config := sm.resolveOp(op)
  reply.Config = config

  return nil
}

func (sm *ShardMaster) resolveOp(op Op) Config {
  seq := sm.px.Max()+1
  sm.log("Resolve init seq=%d", seq)
  sm.px.Start(seq, op)

  timeout := InitTimeout
  time.Sleep(timeout)

  decided,val := sm.px.Status(seq)

  var valOp Op
  if val != nil {
    valOp = val.(Op)
  }

  for !decided || !OpEquals(valOp, op) {
    if (decided && !OpEquals(valOp, op)) || (seq <= sm.lastAppliedSeq) {
      sm.log("Seq=%d already decided", seq)
      seq = sm.px.Max()+1
      sm.px.Start(seq, op)
    }

    sm.log("Retry w/ seq=%d", seq)
    time.Sleep(timeout + RandMTime())
    if timeout < 100 * time.Millisecond {
      timeout *= 2
    }

    decided,val = sm.px.Status(seq)
    if val != nil {
      valOp = val.(Op)
    }
  }

  sm.log("Seq=%d decided!", seq)

  // block until seq op has been applied
  for sm.lastAppliedSeq < seq {
    time.Sleep(InitTimeout)
  }

  sm.px.Done(seq)

  if op.Operation == Query {
    num := op.Args.(QueryArgs).Num
    config,_ := sm.applyQuery(num)

    return *config
  }

  return Config{}
}

func (sm *ShardMaster) applyJoin(joinGid int64, servers []string) (*Config,Err) {
  // Get newest config
  config,num := sm.newestConfig()
  newConfigNum := num+1

  _,exists := config.Groups[joinGid]
  if exists {
    return &Config{},ErrGIDAlreadyJoined
  }

  // Add new gid into groups mapping
  groups := CopyGroups(config.Groups)
  groups[joinGid] = servers

  shards := CopyShards(config.Shards)

  if len(groups) == 1 {
    for i,_ := range shards {
      shards[i] = joinGid
    }
  } else {
    // Redistribute shards
    shardAllocs := MakeShardAllocs(config.Groups, config.Shards)
    sorted := SortShardAllocs(shardAllocs, "dec")

    shardsPerGroup := NShards / len(groups)
    for i := 0; i < shardsPerGroup; i += 1 {
      index := i % len(sorted)
      shardAlloc := sorted[index]

      s := shardAlloc.Shards
      sorted[index].Shards = s[1:len(s)]

      shard := s[0]
      shards[shard] = joinGid
    }
  }

  return MakeConfig(newConfigNum, shards, groups),OK
}

func (sm *ShardMaster) applyLeave(leaveGid int64) (*Config,Err) {
  // Get newest config
  config,num := sm.newestConfig()
  newConfigNum := num+1

  _,exists := config.Groups[leaveGid]
  if !exists {
    return &Config{},ErrGIDAlreadyLeft
  }

  // Remove gid from groups mapping
  groups := CopyGroups(config.Groups)
  delete(groups, leaveGid)

  shards := CopyShards(config.Shards)

  if len(groups) == 0 {
    for i,_ := range shards {
      shards[i] = 0
    }
  } else {
    shardAllocs := MakeShardAllocs(config.Groups, config.Shards)
    sorted := SortShardAllocs(shardAllocs, "inc")

    freeShards := shardAllocs[leaveGid].Shards

    // Assign shard to new gid
    rotate := 0
    for _,shard := range freeShards {
      gid := leaveGid
      for gid == leaveGid {
        gid = sorted[rotate % len(sorted)].GID
        rotate += 1
      }
      shards[shard] = gid
    }
  }

  return MakeConfig(newConfigNum, shards, groups),OK
}

func (sm *ShardMaster) applyMove(movingShard int, newGid int64) (*Config,Err) {
  // Get newest config
  config,num := sm.newestConfig()
  newConfigNum := num+1

  _,exists := config.Groups[newGid]
  if !exists {
    return &Config{},ErrGIDAlreadyLeft
  }

  groups := CopyGroups(config.Groups)
  shards := CopyShards(config.Shards)

  // Move shard into new gid
  shards[movingShard] = newGid

  return MakeConfig(newConfigNum, shards, groups),OK
}

func (sm *ShardMaster) applyQuery(num int) (*Config,Err) {
  // Get newest config if requested config num out of range
  if num < 0 || num >= len(sm.configs) {
    config,_ := sm.newestConfig()
    return &config,OK
  }

  return &sm.configs[num],OK
}

func (sm *ShardMaster) applyOp(op *Op) (*Config,Err) {
  // Return early for a noop
  if op.Operation == Noop {
    return &Config{},ErrNoop
  }

  switch op.Operation {
  case Join:
    args := op.Args.(JoinArgs)
    return sm.applyJoin(args.GID, args.Servers)
  case Leave:
    args := op.Args.(LeaveArgs)
    return sm.applyLeave(args.GID)
  case Move:
    args := op.Args.(MoveArgs)
    return sm.applyMove(args.Shard, args.GID)
  case Query:
    args := op.Args.(QueryArgs)
    return sm.applyQuery(args.Num)
  }

  // Should not reach this point
  return &Config{},ErrInvalid
}

func (sm *ShardMaster) newestConfig() (Config,int) {
  i := len(sm.configs)-1
  return sm.configs[i],i
}

func (sm *ShardMaster) tick() {

  timeout := InitTimeout

  for sm.dead == false {

    seq := sm.lastAppliedSeq+1
    decided,result := sm.px.Status(seq)

    if decided {
      // apply the operation
      op,_ := result.(Op)

      sm.log("Applying %s from seq=%d", op.Operation, seq)
      config,err := sm.applyOp(&op)
      sm.lastAppliedSeq += 1

      sm.mu.Lock()
      if op.Operation != Query && err == OK {
        sm.configs = append(sm.configs, *config)
      }
      sm.mu.Unlock()

      // reset timeout
      timeout = InitTimeout

    } else {
      // sm.log("Retry for seq=%d", seq)

      if timeout >= 1 * time.Second {
        sm.log("Try noop for seq=%d", seq)
        sm.px.Start(seq, Op{Operation: Noop})

        // wait for noop to return
        noopDone := false
        for !noopDone {
          noopDone,_ = sm.px.Status(seq)
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

func (sm *ShardMaster) log(format string, a ...interface{}) (n int, err error) {
  if Debug {
    addr := "Srv#" + strconv.Itoa(sm.me)
    n, err = fmt.Printf(addr + ": " + format + "\n", a...)
  }
  return
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(JoinArgs{})
  gob.Register(LeaveArgs{})
  gob.Register(MoveArgs{})
  gob.Register(QueryArgs{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = make(map[int64][]string)

  sm.lastAppliedSeq = -1

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  go sm.tick()

  return sm
}
