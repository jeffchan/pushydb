package messagebroker

import "time"
import "fmt"

var _ = fmt.Println

const RetryInterval = 100 * time.Millisecond

type Notifier struct {
  servers []string // shardmaster replicas
}

func MakeNotifier(servers []string) *Notifier {
  no := new(Notifier)
  no.servers = servers
  return no
}

func (no *Notifier) NotifyPut(
  key string,
  version int64,
  reqId string,
  val string,
  prev string,
  dohash bool,
  expiration time.Time,
) {

  args := &NotifyPutArgs{
    Key:        key,
    Version:    version,
    ReqId:      reqId,
    Value:      val,
    PrevValue:  prev,
    DoHash:     dohash,
    Expiration: expiration,
  }
  var reply NotifyPutReply

  for {
    // try each known server.
    for _, srv := range no.servers {
      ok := call(srv, "MBServer.NotifyPut", args, &reply)
      if ok && reply.Err == OK {
        return
      }
    }
    time.Sleep(RetryInterval)
  }
}

func (no *Notifier) NotifySubscribe(
  key string,
  version int64,
  reqId string,
  address string,
  unsub bool,
) {

  args := &NotifySubscribeArgs{
    Key:         key,
    Version:     version,
    ReqId:       reqId,
    Address:     address,
    Unsubscribe: unsub,
  }
  var reply NotifySubscribeReply
  for {
    // try each known server.
    for _, srv := range no.servers {
      ok := call(srv, "MBServer.NotifySubscribe", args, &reply)
      if ok && reply.Err == OK {
        return
      }
    }
    time.Sleep(RetryInterval)
  }
}
