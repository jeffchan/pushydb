package messagebroker

import "testing"
import "strconv"
import "os"
import "fmt"
import "time"
import "runtime"
import "math/rand"

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

func cleanupClerk(ck *Clerk) {
  ck.Kill()
}

func cleanup(mbservers []*MBServer) {
  for i := 0; i < len(mbservers); i++ {
    mbservers[i].Kill()
  }
}

func setup(tag string, unreliable bool) ([]string, []*MBServer, func()) {
  runtime.GOMAXPROCS(4)

  const nbrokers = 3
  var mbservers []*MBServer = make([]*MBServer, nbrokers)
  var hosts []string = make([]string, nbrokers)

  for i := 0; i < nbrokers; i++ {
    hosts[i] = port(tag+"m", i)
  }
  for i := 0; i < nbrokers; i++ {
    mbservers[i] = StartServer(hosts, i)
  }

  clean := func() { cleanup(mbservers) }

  return hosts, mbservers, clean
}

func TestBasic(t *testing.T) {
  _, mbservers, clean := setup("basic", false)
  defer clean()

  fmt.Printf("Test: Basic notify -> publish...\n")

  addr := port("basic-clerk", 0)
  publications := make(chan PublishArgs)
  clerk := MakeClerk(addr, publications)
  defer cleanupClerk(clerk)

  pubArgs := PublishArgs{
    Key:   "a",
    Value: "x",
    ReqId: "basic",
  }
  args := &NotifyArgs{
    GID:         0
    Seq:         0
    PublishArgs: pubArgs,
    Subscribers: map[string]bool{addr: true},
  }

  var reply NotifyReply
  mbservers[0].Notify(args, &reply)
  pub := <-publications

  if pub != pubArgs {
    t.Fatalf("Wrong publication; expected %s, got %s", pubArgs, pub)
  }

  fmt.Printf(" ...Passed\n")
}

func TestMany(t *testing.T) {
  _, mbservers, clean := setup("many", false)
  defer clean()

  fmt.Printf("Test: Many notify -> publish...\n")

  addr := port("many-clerk", 0)
  publications := make(chan PublishArgs)
  clerk := MakeClerk(addr, publications)
  defer cleanupClerk(clerk)

  npublish := 10

  pubArgs := make([]PublishArgs, 0, npublish)
  for i := 0; i < npublish; i++ {
    pubArg := PublishArgs{
      Key:   "a",
      Value: strconv.Itoa(i),
      ReqId: "many-" + strconv.Itoa(i+1),
    }
    pubArgs = append(pubArgs, pubArg)
  }

  notifyArgs := make([]*NotifyArgs, 0, npublish)
  for i := 0; i < npublish; i++ {
    notifyArg := &NotifyArgs{
      GID:         0
      Seq:         i + 1,
      PublishArgs: pubArgs[i],
      Subscribers: map[string]bool{addr: true},
    }
    notifyArgs = append(notifyArgs, notifyArg)
  }

  var reply NotifyReply
  for i := 0; i < npublish; i++ {
    mbservers[rand.Int()%len(mbservers)].Notify(notifyArgs[i], &reply)
    pub := <-publications
    if pub != pubArgs[i] {
      t.Fatalf("Wrong publication; expected %s, got %s", pubArgs[i], pub)
    }
  }

  fmt.Printf(" ...Passed\n")
}
