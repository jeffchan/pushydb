package messagebroker

import "testing"
import "strconv"
import "os"
import "fmt"
import "time"
import "runtime"
import "math/rand"

import leveldb "github.com/syndtr/goleveldb/leveldb"

const DB_PATH = "/var/tmp/db/"
var db *leveldb.DB

func setupDB() {
  if db != nil {
    return
  }
  name := "dummy.db"
  deleteDB(name)
  var err error
  db, err = leveldb.OpenFile(DB_PATH+name, nil)
  if err != nil {
    fmt.Printf("Error opening db! %s\n", err)
  }
}

func deleteDB(name string) {
  os.RemoveAll(DB_PATH + name)
}

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

  setupDB()

  const nbrokers = 3
  var mbservers []*MBServer = make([]*MBServer, nbrokers)
  var hosts []string = make([]string, nbrokers)

  for i := 0; i < nbrokers; i++ {
    hosts[i] = port(tag+"m", i)
  }
  for i := 0; i < nbrokers; i++ {
    mbservers[i] = StartServer(hosts, i, db)
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

  subArgs := &NotifySubscribeArgs{
    Key:         "a",
    Version:     1,
    ReqId:       "basic1",
    Address:     addr,
    Unsubscribe: false,
  }

  mbservers[0].NotifySubscribe(subArgs, &NotifySubscribeReply{})

  putArgs := &NotifyPutArgs{
    Key:     "a",
    Version: 2,
    ReqId:   "basic2",
    Value:   "x",
  }

  mbservers[0].NotifyPut(putArgs, &NotifyPutReply{})

  unsubArgs := &NotifySubscribeArgs{
    Key:         "a",
    Version:     3,
    ReqId:       "basic3",
    Address:     addr,
    Unsubscribe: true,
  }

  mbservers[0].NotifySubscribe(unsubArgs, &NotifySubscribeReply{})

  pub := <-publications
  if pub.Type != Subscribe {
    t.Fatalf("Not Subscribe type")
  }

  pub = <-publications
  if pub.PutValue() != "x" {
    t.Fatalf("Wrong publication")
  }

  pub = <-publications
  if pub.Type != Subscribe {
    t.Fatalf("Wrong publication")
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

  subArgs := &NotifySubscribeArgs{
    Key:         "a",
    Version:     1,
    ReqId:       "many-",
    Address:     addr,
    Unsubscribe: false,
  }

  mbservers[0].NotifySubscribe(subArgs, &NotifySubscribeReply{})

  pub := <-publications
  if pub.Type != Subscribe {
    t.Fatalf("Not Subscribe type")
  }

  npublish := 10

  putArgs := make([]NotifyPutArgs, 0, npublish)
  for i := 0; i < npublish; i++ {
    putArg := NotifyPutArgs{
      Key:     "a",
      Version: int64(i + 2),
      ReqId:   "many-" + strconv.Itoa(i+2),
      Value:   strconv.Itoa(i + 2),
    }
    putArgs = append(putArgs, putArg)
  }

  for i := 0; i < npublish; i++ {
    mbservers[rand.Int()%len(mbservers)].NotifyPut(&putArgs[i], &NotifyPutReply{})
  }

  for i := 0; i < npublish; i++ {
    pub = <-publications
    if pub.PutValue() != putArgs[i].Value {
      t.Fatalf("Wrong pub, expected=%s, got=%s", putArgs[i].Value, pub.PutValue())
    }
  }

  fmt.Printf(" ...Passed\n")
}
