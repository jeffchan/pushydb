package shardkv

import "shardmaster"
import "time"
import "sync"
import "fmt"
import "strconv"
import "crypto/rand"
import "math/big"
import "messagebroker"

const (
  ClientLog = false
)

type Clerk struct {
  mu      sync.Mutex // one RPC at a time
  sm      *shardmaster.Clerk
  config  shardmaster.Config
  id      string
  counter uint64

  addr    string
  mb      *messagebroker.Clerk
  Receive chan messagebroker.PublishArgs
}

func MakeClerk(shardmasters []string) *Clerk {
  ck := new(Clerk)
  ck.sm = shardmaster.MakeClerk(shardmasters)
  ck.id = strconv.Itoa(int(nrand()))
  ck.counter = 0

  ck.addr = "/var/tmp/824-" + ck.id
  ck.Receive = make(chan messagebroker.PublishArgs)
  ck.mb = messagebroker.MakeClerk(ck.addr, ck.Receive)
  return ck
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

func (ck *Clerk) reqId() string {
  ck.counter++
  return ck.id + ":" + strconv.Itoa(int(ck.counter))
}

func (ck *Clerk) log(format string, a ...interface{}) (n int, err error) {
  if ClientLog {
    addr := "ClientID#" + ck.id + "|"
    reqId := "ReqId#" + strconv.FormatUint(ck.counter, 10)
    n, err = fmt.Printf(addr+reqId+" ** "+format+"\n", a...)
  }
  return
}

func (ck *Clerk) Kill() {
  ck.mb.Kill()
}

//
// Subscribe to changes to a given key
// Recieve changes on Receive channel
// Set unsub = true to unsubscribe
//
func (ck *Clerk) SubscribeExt(key string, unsub bool) {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  reqId := ck.reqId()

  for {
    shard := key2shard(key)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &SubscribeArgs{}
        args.Key = key
        args.Address = ck.addr
        args.Unsubscribe = unsub
        args.ReqId = reqId
        var reply SubscribeReply
        ok := call(srv, "ShardKV.Subscribe", args, &reply)
        if ok && reply.Err == OK {
          return
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) Subscribe(key string) {
  ck.SubscribeExt(key, false)
}
func (ck *Clerk) Unsubscribe(key string) {
  ck.SubscribeExt(key, true)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  reqId := ck.reqId()

  for {
    shard := key2shard(key)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &GetArgs{}
        args.Key = key
        args.ReqId = reqId
        var reply GetReply
        ok := call(srv, "ShardKV.Get", args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          return reply.Value
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
  return ""
}

func (ck *Clerk) PutExt(
  key string,
  value string,
  dohash bool,
  ttl time.Duration,
) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  reqId := ck.reqId()

  for {
    shard := key2shard(key)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &PutArgs{}
        args.Key = key
        args.Value = value
        args.DoHash = dohash
        args.TTL = ttl
        args.ReqId = reqId
        var reply PutReply
        ok := call(srv, "ShardKV.Put", args, &reply)
        if ok && reply.Err == OK {
          return reply.PreviousValue
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false, 0)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true, 0)
  return v
}
