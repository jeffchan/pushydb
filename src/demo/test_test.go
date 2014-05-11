package demo

import "testing"
import "strconv"
import "os"
import "fmt"
import "time"
import "runtime"
import "messagebroker"

var _ = time.Millisecond

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "mb-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func cleanupClerk(ck *messagebroker.Clerk) {
  ck.Kill()
}

func cleanup(mbservers []*messagebroker.MBServer) {
  for i := 0; i < len(mbservers); i++ {
    mbservers[i].Kill()
  }
}

func setup(tag string, unreliable bool) ([]string, []*messagebroker.MBServer, func()) {
  runtime.GOMAXPROCS(4)

  const nbrokers = 3
  var mbservers []*messagebroker.MBServer = make([]*messagebroker.MBServer, nbrokers)
  var hosts []string = make([]string, nbrokers)

  for i := 0; i < nbrokers; i++ {
    hosts[i] = port(tag+"m", i)
  }
  for i := 0; i < nbrokers; i++ {
    mbservers[i] = messagebroker.StartServer(hosts, i)
  }

  clean := func() { cleanup(mbservers) }

  return hosts, mbservers, clean
}

func TestBasic(t *testing.T) {
  _, mbservers, clean := setup("basic", false)
  defer clean()

  fmt.Printf("Test: Basic notify -> publish...\n")

  demo := MakeDemo()

  subArgs := &messagebroker.NotifySubscribeArgs{
    Key:         "a",
    Version:     1,
    ReqId:       "basic1",
    Address:     demo.addr,
    Unsubscribe: false,
  }

  mbservers[0].NotifySubscribe(subArgs, &messagebroker.NotifySubscribeReply{})

  for i := 2; i < 22; i++ {
    putArgs := &messagebroker.NotifyPutArgs{
      Key:        "a",
      Version:    int64(i),
      ReqId:      "basic" + strconv.Itoa(i),
      Value:      strconv.Itoa(i),
    }

    mbservers[0].NotifyPut(putArgs, &messagebroker.NotifyPutReply{})

    fmt.Printf("Notifying MB's of new op\n")

    time.Sleep(time.Second)
  }

  unsubArgs := &messagebroker.NotifySubscribeArgs{
    Key:         "a",
    Version:     22,
    ReqId:       "basic3",
    Address:     demo.addr,
    Unsubscribe: true,
  }

  mbservers[0].NotifySubscribe(unsubArgs, &messagebroker.NotifySubscribeReply{})

  fmt.Printf(" ...Passed\n")
}
