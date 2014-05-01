package kvpaxos

import "hash/fnv"

const (
  OK                = "OK"
  ErrNoKey          = "ErrNoKey"
  ErrNoOp           = "ErrNoOp"
  ErrAlreadyApplied = "ErrAlreadyApplied"
  ErrInvalid        = "ErrInvalid"
)

type Err string

const (
  Put     = "Put"
  PutHash = "PutHash"
  Get     = "Get"
  Noop    = "Noop"
)

type Operation string

type PutArgs struct {
  Key    string
  Value  string
  DoHash bool // For PutHash
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

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}
