package messagebroker

import "time"

const RetryInterval = 100 * time.Millisecond

type Notifier struct {
  servers []string // shardmaster replicas
}

func MakeNotifier(servers []string) *Notifier {
  no := new(Notifier)
  no.servers = servers
  return no
}

func (no *Notifier) Notify(
  gid int64,
  seq int,
  key string,
  val string,
  reqId string,
  subscribers map[string]bool,
) {

  args := &NotifyArgs{
    GID: gid,
    Seq: seq,
    PublishArgs: PublishArgs{
      Key:   key,
      Value: val,
      ReqId: reqId,
    },
    Subscribers: subscribers,
  }
  var reply NotifyReply

  for {
    // try each known server.
    for _, srv := range no.servers {
      ok := call(srv, "MBServer.Notify", args, &reply)
      if ok && reply.Err == OK {
        return
      }
    }
    time.Sleep(RetryInterval)
  }
}
