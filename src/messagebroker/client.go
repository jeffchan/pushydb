package messagebroker

import "net"
import "net/rpc"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import crand "crypto/rand"
import "math/big"
import "encoding/gob"
import "time"

const (
  ClientLog = false
)

type Clerk struct {
  mu         sync.Mutex // one RPC at a time
  l          net.Listener
  me         string
  dead       bool // for testing
  unreliable bool // for testing

  dups    map[string]bool
  publish chan PublishArgs
  pending []*PublishArgs
}

func GetValue(args *PublishArgs) string {
  if args.Type == Put {
    return args.PutArgs.Value
  }
  return ""
}

func GetSubscribedKey(args *PublishArgs) string {
  if args.Type == Subscribe {
    return args.SubscribeArgs.Key
  }
  return ""
}

func MakeClerk(me string, publish chan PublishArgs) *Clerk {
  ck := new(Clerk)
  ck.me = me
  ck.dups = make(map[string]bool)
  ck.publish = publish

  rpcs := rpc.NewServer()
  rpcs.Register(ck)

  os.Remove(ck.me)
  l, e := net.Listen("unix", ck.me)
  if e != nil {
    fmt.Println("listener error: ", e)
  }
  ck.l = l

  gob.Register(PublishArgs{})

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for ck.dead == false {
      conn, err := ck.l.Accept()
      if err == nil && ck.dead == false {
        if ck.unreliable && (rand.Int63()%1000) < 100 {
          // discard the request.
          conn.Close()
        } else if ck.unreliable && (rand.Int63()%1000) < 200 {
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
      if err != nil && ck.dead == false {
        fmt.Printf("MBClerk(%v) accept: %v\n", ck.me, err.Error())
        ck.Kill()
      }
    }
  }()

  go ck.process()

  return ck
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return x
}

func (ck *Clerk) Publish(args *PublishArgs, reply *PublishReply) error {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  reqId := args.ReqId
  _, ok := ck.dups[reqId]
  if !ok {
    ck.dups[reqId] = true
    ck.pending = append(ck.pending, args)
  }

  reply.Err = OK
  return nil
}

func (ck *Clerk) process() {
  for !ck.dead {
    if len(ck.pending) > 0 {
      notification := ck.pending[0]
      ck.pending = ck.pending[1:]
      ck.publish <- *notification
    }
    time.Sleep(50 * time.Millisecond)
  }
}

func (ck *Clerk) Kill() {
  ck.dead = true
  ck.l.Close()
}

func (ck *Clerk) log(format string, a ...interface{}) (n int, err error) {
  if ClientLog {
    addr := "MBClerk#" + ck.me
    n, err = fmt.Printf(addr+" ** "+format+"\n", a...)
  }
  return
}
