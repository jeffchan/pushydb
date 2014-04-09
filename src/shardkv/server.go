package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "strings"

func ParseReqId(reqId string) (string,uint64) {
  s := strings.Split(reqId, ":")
  clientId := s[0]
  reqNum,_ := strconv.ParseUint(s[1], 10, 64)
  return clientId,reqNum
}

const (
  Put = "Put"
  PutHash = "PutHash"
  Get = "Get"
  Reconfig = "Reconfig"
  Noop = "Noop"
)
type Operation string

type Op struct {
  Operation Operation
  Args interface{}
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID
  config shardmaster.Config
}

const ServerLog = true
func (kv *ShardKV) log(format string, a ...interface{}) (n int, err error) {
  if ServerLog {
    addr := "Srv#" + strconv.Itoa(kv.me)
    gid := "GID#" + strconv.FormatInt(kv.gid, 10)
    n, err = fmt.Printf(addr + "|" + gid + " >> " + format + "\n", a...)
  }
  return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  key := args.Key
  clientId,_ := ParseReqId(args.ReqId)

  kv.log("Get receive, key=%s, clientId=%s", key, clientId)

  val := ""
  kv.log("Get return, key=%s, val=%s, clientId=%s", key, val, clientId)

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  key := args.Key
  val := args.Value
  clientId,_ := ParseReqId(args.ReqId)

  kv.log("Put receive, key=%s, val=%s, clientId=%s", key, val, clientId)

  prev := ""
  kv.log("Put return, key=%s, val=%s, prev=%s, clientId=%s", key, val, prev, clientId)

  return nil
}

func (kv *ShardKV) reconfig() {

}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  // Query shardmaster for latest config
  newConfig := kv.sm.Query(-1)

  // Reconfigure if there's a new configuration
  if newConfig.Num > kv.config.Num {
    kv.reconfig()
  }
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.config = kv.sm.Query(-1)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
