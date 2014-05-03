package shardkv

import "hash/fnv"
import "time"

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
  Value       string
  Expiration  time.Time
  Subscribers map[string]bool
}

func NewEntry() *Entry {
  return &Entry{
    Subscribers: make(map[string]bool),
  }
}

func CopyEntry(src *Entry) *Entry {
  return &Entry{
    Value:       src.Value,
    Expiration:  src.Expiration,
    Subscribers: CopySubscribers(src.Subscribers),
  }
}

func CopySubscribers(src map[string]bool) map[string]bool {
  dst := make(map[string]bool)
  for k, v := range src {
    dst[k] = v
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
