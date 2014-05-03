package messagebroker

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
)

type Err string

const (
  Notify = "Notify"
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
  Subscriber  map[string]bool
}

type NotifyReply struct {
  Err Err
}
