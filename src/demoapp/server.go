package main

import "github.com/go-martini/martini"
import "messagebroker"
import "shardkv"
import "shardmaster"
import "strconv"
import "github.com/martini-contrib/render"
import "os"
import "runtime"
import "fmt"

// game plan: use pushydb as actual db
// set up infrastructure on main() call, spawn goroutine to listen on channel
// interface: form: subscribe to a key
// form: put to a key
// view: see realtime push from server
// how to get server > client ?
// easiest way- just do long-polling. Could use Gorilla socket...may be hard

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "skv-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func cleanupClerk(ck *shardkv.Clerk) {
  ck.Kill()
}

func mbcleanup(mba []*messagebroker.MBServer) {
  for i := 0; i < len(mba); i++ {
    if mba[i] != nil {
      mba[i].Kill()
    }
  }
}

func mcleanup(sma []*shardmaster.ShardMaster) {
  for i := 0; i < len(sma); i++ {
    if sma[i] != nil {
      sma[i].Kill()
    }
  }
}

func cleanup(sa [][]*shardkv.ShardKV) {
  for i := 0; i < len(sa); i++ {
    for j := 0; j < len(sa[i]); j++ {
      sa[i][j].Kill()
    }
  }
}

func setup(tag string, unreliable bool, sudden bool) ([]string, []int64, [][]string, [][]*shardkv.ShardKV, func(), []string) {
  runtime.GOMAXPROCS(4)

  const nmasters = 3
  var sma []*shardmaster.ShardMaster = make([]*shardmaster.ShardMaster, nmasters)
  var smh []string = make([]string, nmasters)
  // defer mcleanup(sma)
  for i := 0; i < nmasters; i++ {
    smh[i] = port(tag+"m", i)
  }
  for i := 0; i < nmasters; i++ {
    sma[i] = shardmaster.StartServer(smh, i)
  }

  var mba []*messagebroker.MBServer = make([]*messagebroker.MBServer, nmasters)
  var mbh []string = make([]string, nmasters)
  for i := 0; i < nmasters; i++ {
    mbh[i] = port(tag+"messagebroker", i)
  }
  for i := 0; i < nmasters; i++ {
    mba[i] = messagebroker.StartServer(mbh, i)
  }

  const ngroups = 3                 // replica groups
  const nreplicas = 3               // servers per group
  gids := make([]int64, ngroups)    // each group ID
  ha := make([][]string, ngroups)   // ShardKV ports, [group][replica]
  sa := make([][]*shardkv.ShardKV, ngroups) // ShardKVs
  // defer cleanup(sa)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    sa[i] = make([]*shardkv.ShardKV, nreplicas)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      ha[i][j] = port(tag+"s", (i*nreplicas)+j)
    }
    for j := 0; j < nreplicas; j++ {
      sa[i][j] = shardkv.StartServer(gids[i], smh, ha[i], j, mbh)
    }
  }

  clean := func() { cleanup(sa); mcleanup(sma); mbcleanup(mba) }
  return smh, gids, ha, sa, clean, mbh
}

func listener(ck *shardkv.Clerk) {
  for {
    v := <- ck.Receive
    fmt.Printf("%+v", v)
    // send v on socket
  }
}

func main() {
  smh, gids, ha, _, clean, _ := setup("demo", false, false)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := shardkv.MakeClerk(smh)
  defer cleanupClerk(ck)

  go listener(ck)
  go h.run()

  m := martini.Classic()
  m.Use(render.Renderer())

  m.Get("/", func(r render.Render) {
    r.HTML(200, "index", "")
  })

  m.Post("/ws", wsHandler)

  m.Run()
}
