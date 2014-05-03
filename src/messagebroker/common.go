package messagebroker

import "net/rpc"
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
  ErrAlreadyApplied = "ErrAlreadyApplied"
  ErrInvalid        = "ErrInvalid"
  ErrNoOp           = "ErrNoOp"
)

type Err string

const (
  Notify = "Notify"
  Noop   = "Noop"
)

type Operation string

type Op struct {
  Operation Operation
  Args      interface{}
  ReqId     string
}

type PublishArgs struct {
  Key   string
  Value string
  ReqId string
}

type PublishReply struct {
  Err Err
}

type NotifyArgs struct {
  PublishArgs PublishArgs
  Subscribers map[string]bool
}

type NotifyReply struct {
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