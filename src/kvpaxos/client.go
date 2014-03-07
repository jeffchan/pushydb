package kvpaxos

import "net/rpc"
import "fmt"
import "time"
import mathrand "math/rand"
import "crypto/rand"
import "math/big"
import "strconv"
import "sync"

type Clerk struct {
  servers []string
  id string
  mu sync.Mutex
  counter uint64
}

func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  ck.id = strconv.Itoa(int(nrand()))
  ck.counter = 0
  return ck
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

const ClerkLog = false
func (ck *Clerk) log(format string, a ...interface{}) (n int, err error) {
  if ClerkLog {
    n, err = fmt.Printf(ck.id + ": " + format + "\n", a...)
  }
  return
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

func (ck *Clerk) server(try int) string {
  index := try % len(ck.servers)
  return ck.servers[index]
}

func (ck *Clerk) reqId() string {
  ck.mu.Lock()
  defer ck.mu.Unlock()
  ck.counter++
  return ck.id + ":" + strconv.Itoa(int(ck.counter))
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  index := mathrand.Int() % len(ck.servers)
  server := ck.server(index)

  ck.log("Get: key=%s", key)

  var reply GetReply
  args := GetArgs{Key: key, ReqId: ck.reqId()}

  for {
    ok := call(server, "KVPaxos.Get", &args, &reply)
    err := reply.Err

    if err == OK || err == ErrNoKey {
      return reply.Value
    } else if !ok {
      index++
      server = ck.server(index)
    }

    ck.log("Get: key=%s, server=%s", key, server)
    time.Sleep(TickInterval)
  }
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  index := mathrand.Int() % len(ck.servers)
  server := ck.server(index)

  ck.log("Put: key=%s, val=%s, hash=%t", key, value, dohash)

  var reply PutReply
  args := PutArgs{Key: key, Value: value, DoHash: dohash, ReqId: ck.reqId()}

  for {
    ok := call(server, "KVPaxos.Put", &args, &reply)
    err := reply.Err

    if err == OK {
      return reply.PreviousValue
    } else if !ok {
      index++
      server = ck.server(index)
    }

    ck.log("Put: key=%s, val=%s, server=%s", key, value, server)
    time.Sleep(TickInterval)
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
