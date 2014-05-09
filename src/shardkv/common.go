package shardkv

import "hash/fnv"
import "time"
import "net/rpc"
import "shardmaster"
import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK                = "OK"
  ErrNoKey          = "ErrNoKey"
  ErrWrongGroup     = "ErrWrongGroup"
  ErrAlreadyApplied = "ErrAlreadyApplied"
  ErrNoOp           = "ErrNoOp"
  ErrInvalid        = "ErrInvalid"
  ErrWrongView      = "ErrWrongView"
)

type Err string

type Entry struct {
  Version     int64
  Value       string
  Expiration  time.Time
  Subscribers map[string]bool
}

func NewEntry() *Entry {
  return &Entry{
    Version:     0,
    Subscribers: make(map[string]bool),
  }
}

func (entry *Entry) Clone() *Entry {
  return &Entry{
    Version:     entry.Version,
    Value:       entry.Value,
    Expiration:  entry.Expiration,
    Subscribers: CopySubscribers(entry.Subscribers),
  }
}

func (entry *Entry) IsExpired(refTime time.Time) bool {
  return !entry.Expiration.IsZero() && refTime.After(entry.Expiration)
}

func CopySubscribers(src map[string]bool) map[string]bool {
  dst := make(map[string]bool)
  for k, v := range src {
    if v {
      dst[k] = v
    }
  }
  return dst
}

const (
  Put       = "Put"
  Get       = "Get"
  Reconfig  = "Reconfig"
  Subscribe = "Subscribe"
  Noop      = "Noop"
)

type Operation string

type Op struct {
  Operation Operation
  Args      interface{}
  ReqId     string
  Timestamp time.Time
  ConfigNum int
}

type Result struct {
  ReqId string
  Val   string
  Err   Err
}

type SubscribeArgs struct {
  Key         string
  Address     string
  Unsubscribe bool // True to unsubscribe
  ReqId       string
}

type SubscribeReply struct {
  Err Err
}

type PutArgs struct {
  Key    string
  Value  string
  DoHash bool // For PutHash
  TTL    time.Duration
  ReqId  string
}

type PutReply struct {
  Err           Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key   string
  ReqId string
}

type GetReply struct {
  Err   Err
  Value string
}

type ReconfigArgs struct {
  FromConfigNum int
  ToConfigNum   int
}

type TransferArgs struct {
  ConfigNum int
  Shard     int
}

type TransferReply struct {
  Err   Err
  Table map[string]*Entry
  Reqs  map[string]*Result
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
  args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= shardmaster.NShards
  return shard
}
