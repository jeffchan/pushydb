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

  for i := 0; i < 20; i++ {
    pubArgs := messagebroker.PublishArgs{
      Key:        "a",
      Value:      strconv.Itoa(i),
      ReqId:      "basic",
      Expiration: time.Now(),
    }
    args := &messagebroker.NotifyArgs{
      Version:     int64(i),
      PublishArgs: pubArgs,
      Subscribers: map[string]bool{demo.addr: true},
    }

    fmt.Printf("Notifying MB's of new op\n")

    var reply messagebroker.NotifyReply
    mbservers[0].Notify(args, &reply)

    time.Sleep(time.Second)
  }

  fmt.Printf(" ...Passed\n")
}
