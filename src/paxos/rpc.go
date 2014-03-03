package paxos

const (
  OK = "OK"
  Reject = "Reject"
)
type Err string

//
// Prepare
//
type PrepareArgs struct {
  Seq int
  N int64
}

type PrepareReply struct {
  Seq int
  Err Err
  HighestDoneSeq int
  N int64
  Val interface{}
}

//
// Accept
//
type AcceptArgs struct {
  Seq int
  N int64
  Val interface{}
}

type AcceptReply struct {
  Seq int
  Err Err
  HighestDoneSeq int
  N int64
}

//
// Decided
//
type DecidedArgs struct {
  Seq int
  Val interface{}
}

type DecidedReply struct {
  Seq int
  Err Err
  HighestDoneSeq int
}