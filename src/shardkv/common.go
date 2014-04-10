package shardkv
import "hash/fnv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrAlreadyApplied = "ErrAlreadyApplied"
  ErrNoOp = "ErrNoOp"
  ErrInvalid = "ErrInvalid"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  ReqId string
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  ReqId string
}

type GetReply struct {
  Err Err
  Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}
