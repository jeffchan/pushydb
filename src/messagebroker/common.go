package messagebroker

import "net/rpc"
import "fmt"
import "time"

type Err string

const (
  OK                = "OK"
  ErrAlreadyApplied = "ErrAlreadyApplied"
  ErrInvalid        = "ErrInvalid"
  ErrNoOp           = "ErrNoOp"
  ErrOutOfOrder     = "ErrOutOfOrder"
)

type Operation string

const (
  Noop            = "Noop"
  NotifyPut       = "NotifyPut"
  NotifySubscribe = "NotifySubscribe"
)

type Op struct {
  Operation Operation
  Args      interface{}
  Key       string
  Version   int64
}

type PublishType int

const (
  Subscribe = iota
  Put       = iota
)

type PublishArgs struct {
  Type          PublishType
  ReqId         string
  SubscribeArgs NotifySubscribeArgs
  PutArgs       NotifyPutArgs
}

type PublishReply struct {
  Err Err
}

func (args *PublishArgs) PutValue() string {
  if args.Type == Put {
    return args.PutArgs.Value
  }
  return ""
}

func (args *PublishArgs) Key() string {
  if args.Type == Subscribe {
    return args.SubscribeArgs.Key
  }
  if args.Type == Put {
    return args.PutArgs.Key
  }
  return ""
}

func (args *PublishArgs) Addr() string {
  if args.Type == Subscribe {
    return args.SubscribeArgs.Address
  }
  return ""
}

type NotifySubscribeArgs struct {
  Key     string
  Version int64
  ReqId   string

  Address     string
  Unsubscribe bool // True to unsubscribe
}

type NotifySubscribeReply struct {
  Err Err
}

type NotifyPutArgs struct {
  Key     string
  Version int64
  ReqId   string

  Value      string // Before PutHash (if any)
  PrevValue  string
  DoHash     bool // For PutHash
  Expiration time.Time
}

type NotifyPutReply struct {
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
