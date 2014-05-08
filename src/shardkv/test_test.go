package shardkv

import "testing"
import "shardmaster"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "sync"
import "math/rand"
import "messagebroker"

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

func NextValue(hprev string, val string) string {
  h := hash(hprev + val)
  return strconv.Itoa(int(h))
}

func cleanupClerk(ck *Clerk) {
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

func cleanup(sa [][]*ShardKV) {
  for i := 0; i < len(sa); i++ {
    for j := 0; j < len(sa[i]); j++ {
      sa[i][j].kill()
    }
  }
}

func setup(tag string, unreliable bool, sudden bool) ([]string, []int64, [][]string, [][]*ShardKV, func(), []string) {
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
  sa := make([][]*ShardKV, ngroups) // ShardKVs
  // defer cleanup(sa)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    sa[i] = make([]*ShardKV, nreplicas)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      ha[i][j] = port(tag+"s", (i*nreplicas)+j)
    }
    for j := 0; j < nreplicas; j++ {
      // if sudden {
      //   var procAttr os.ProcAttr
      //   procAttr.Files = []*os.File{nil, os.Stdout, os.Stderr}
      //   process, err := os.StartProcess(os.Args[1], os.Args[1:], &procAttr)
      //   if err != nil {
      //     println("start process failed:" + err.String())
      //     return
      //   }
      //   _, err = process.Wait(0)
      // } else {
      sa[i][j] = StartServer(gids[i], smh, ha[i], j, mbh)
      sa[i][j].unreliable = unreliable
      // }
    }
  }

  clean := func() { cleanup(sa); mcleanup(sma); mbcleanup(mba) }
  return smh, gids, ha, sa, clean, mbh
}

/*************************************************
******************ORIGINAL TESTS*****************
*************************************************/

func TestBasic(t *testing.T) {
  smh, gids, ha, _, clean, _ := setup("basic", false, false)
  defer clean()

  fmt.Printf("Test: Basic Join/Leave ...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  defer cleanupClerk(ck)

  ck.Put("a", "x")
  v := ck.PutHash("a", "b")
  if v != "x" {
    t.Fatalf("Puthash got wrong value")
  }
  ov := NextValue("x", "b")
  if ck.Get("a") != ov {
    t.Fatalf("Get got wrong value")
  }

  keys := make([]string, 10)
  vals := make([]string, len(keys))
  for i := 0; i < len(keys); i++ {
    keys[i] = strconv.Itoa(rand.Int())
    vals[i] = strconv.Itoa(rand.Int())
    ck.Put(keys[i], vals[i])
  }

  // are keys still there after joins?
  for g := 1; g < len(gids); g++ {
    mck.Join(gids[g], ha[g])
    time.Sleep(1 * time.Second)
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }

  // are keys still there after leaves?
  for g := 0; g < len(gids)-1; g++ {
    mck.Leave(gids[g])
    time.Sleep(1 * time.Second)
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }

  fmt.Printf("  ... Passed\n")
}

func TestMove(t *testing.T) {
  smh, gids, ha, _, clean, _ := setup("move", false, false)
  defer clean()

  fmt.Printf("Test: Shards really move ...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  defer cleanupClerk(ck)

  // insert one key per shard
  for i := 0; i < shardmaster.NShards; i++ {
    ck.Put(string('0'+i), string('0'+i))
  }

  // add group 1.
  mck.Join(gids[1], ha[1])
  time.Sleep(5 * time.Second)

  // check that keys are still there.
  for i := 0; i < shardmaster.NShards; i++ {
    if ck.Get(string('0'+i)) != string('0'+i) {
      t.Fatalf("missing key/value")
    }
  }

  // remove sockets from group 0.
  for i := 0; i < len(ha[0]); i++ {
    os.Remove(ha[0][i])
  }

  count := 0
  var mu sync.Mutex
  for i := 0; i < shardmaster.NShards; i++ {
    go func(me int) {
      myck := MakeClerk(smh)
      defer cleanupClerk(myck)
      v := myck.Get(string('0' + me))
      if v == string('0'+me) {
        mu.Lock()
        count++
        mu.Unlock()
      } else {
        t.Fatalf("Get(%v) yielded %v\n", i, v)
      }
    }(i)
  }

  time.Sleep(10 * time.Second)

  if count > shardmaster.NShards/3 && count < 2*(shardmaster.NShards/3) {
    fmt.Printf("  ... Passed\n")
  } else {
    t.Fatalf("%v keys worked after killing 1/2 of groups; wanted %v",
      count, shardmaster.NShards/2)
  }
}

func TestLimp(t *testing.T) {
  smh, gids, ha, sa, clean, _ := setup("limp", false, false)
  defer clean()

  fmt.Printf("Test: Reconfiguration with some dead replicas ...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  defer cleanupClerk(ck)

  ck.Put("a", "b")
  if ck.Get("a") != "b" {
    t.Fatalf("got wrong value")
  }

  for g := 0; g < len(sa); g++ {
    sa[g][rand.Int()%len(sa[g])].kill()
  }

  keys := make([]string, 10)
  vals := make([]string, len(keys))
  for i := 0; i < len(keys); i++ {
    keys[i] = strconv.Itoa(rand.Int())
    vals[i] = strconv.Itoa(rand.Int())
    ck.Put(keys[i], vals[i])
  }

  // are keys still there after joins?
  for g := 1; g < len(gids); g++ {
    mck.Join(gids[g], ha[g])
    time.Sleep(1 * time.Second)
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }

  // are keys still there after leaves?
  for g := 0; g < len(gids)-1; g++ {
    mck.Leave(gids[g])
    time.Sleep(2 * time.Second)
    for i := 0; i < len(sa[g]); i++ {
      sa[g][i].kill()
    }
    for i := 0; i < len(keys); i++ {
      v := ck.Get(keys[i])
      if v != vals[i] {
        t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      ck.Put(keys[i], vals[i])
    }
  }

  fmt.Printf("  ... Passed\n")
}

func doConcurrent(t *testing.T, unreliable bool, subscribe bool) {
  smh, gids, ha, _, clean, _ := setup("conc"+strconv.FormatBool(unreliable)+"-sub="+strconv.FormatBool(subscribe), unreliable, false)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  for i := 0; i < len(gids); i++ {
    mck.Join(gids[i], ha[i])
  }

  const npara = 11
  var ca [npara]chan bool
  for i := 0; i < npara; i++ {
    ca[i] = make(chan bool)
    go func(me int) {
      ok := true
      defer func() { ca[me] <- ok }()
      ck := MakeClerk(smh)
      defer cleanupClerk(ck)
      mymck := shardmaster.MakeClerk(smh)
      key := strconv.Itoa(me)
      last := ""

      if subscribe {
        ck.Subscribe(key)
        publish := <-ck.Receive
        if publish.Key() != key {
          t.Fatalf("Subscribed to wrong key")
        }
      }

      count := 3
      vals := make([]string, 0)
      for iters := 0; iters < count; iters++ {
        nv := strconv.Itoa(rand.Int())
        v := ck.PutHash(key, nv)
        if v != last {
          ok = false
          t.Fatalf("PutHash(%v) expected %v got %v\n", key, last, v)
        }
        last = NextValue(last, nv)
        v = ck.Get(key)
        if v != last {
          ok = false
          t.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
        }

        vals = append(vals, last)

        mymck.Move(rand.Int()%shardmaster.NShards,
          gids[rand.Int()%len(gids)])

        time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
      }

      if subscribe {
        var publish messagebroker.PublishArgs

        for iters := 0; iters < count; iters++ {
          publish = <-ck.Receive
          if vals[iters] != publish.PutValue() {
            t.Fatalf("Pub/sub received=%s, expected=%s", publish, vals[iters])
          }
        }

        ck.Unsubscribe(key)
        publish = <-ck.Receive
        if publish.Key() != key {
          t.Fatalf("Unsubscribed from wrong key")
        }
        close(ck.Receive)
      }

    }(i)
  }

  for i := 0; i < npara; i++ {
    x := <-ca[i]
    if x == false {
      t.Fatalf("something is wrong")
    }
  }
}

func TestConcurrent(t *testing.T) {
  fmt.Printf("Test: Concurrent Put/Get/Move + Pub/Sub ...\n")
  doConcurrent(t, false, true)
  fmt.Printf("  ... Passed\n")
}

func TestConcurrentUnreliable(t *testing.T) {
  fmt.Printf("Test: Concurrent Put/Get/Move + Pub/Sub (unreliable) ...\n")
  doConcurrent(t, true, true)
  fmt.Printf("  ... Passed\n")
}

/*************************************************
*******************EXPIRY TESTS******************
*************************************************/

func TestExpiryBasic(t *testing.T) {
  smh, gids, ha, _, clean, _ := setup("expiry-move", false, false)
  defer clean()

  fmt.Printf("Test: Expiry Basic ...\n")
  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  defer cleanupClerk(ck)

  ttl := 2 * time.Second
  ck.PutExt("a", "x", false, ttl)

  v := ck.Get("a")
  if v != "x" {
    t.Fatalf("Get got wrong value")
  }
  time.Sleep(ttl)

  ov := ck.Get("a")
  if ov != "" {
    t.Fatalf("Get got value, should've expired")
  }

  ov = ck.PutHash("a", "b")
  if ov != "" {
    t.Fatalf("Put got value, should've expired")
  }

  fmt.Printf("  ... Passed\n")
}

func TestExpiryMove(t *testing.T) {
  smh, gids, ha, _, clean, _ := setup("expiry-move", false, false)
  defer clean()

  fmt.Printf("Test: Expiry Multiple Move ...\n")
  mck := shardmaster.MakeClerk(smh)
  for i := 0; i < len(gids); i++ {
    mck.Join(gids[i], ha[i])
  }

  ck := MakeClerk(smh)
  defer cleanupClerk(ck)

  ttl := 1 * time.Second

  for i := 0; i < shardmaster.NShards; i++ {
    key := string('0' + i)
    val := string('0' + i)

    ck.PutExt(key, val, false, ttl)

    v := ck.Get(key)
    if v != val {
      t.Fatalf("Get got wrong value, expected=%s, got=%s", val, v)
    }
    time.Sleep(ttl)

    ov := ck.Get(key)
    if ov != "" {
      t.Fatalf("Get got value, should've expired")
    }

    mck.Move(0, gids[rand.Int()%len(gids)])
  }

  fmt.Printf("  ... Passed\n")
}

/*************************************************
*******************PUBSUB TESTS******************
*************************************************/

func TestPubSubJoin(t *testing.T) {
  smh, gids, ha, _, clean, _ := setup("pubsub-join", false, false)
  defer clean()

  fmt.Printf("Test: Pub/Sub Join ...\n")
  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)
  defer cleanupClerk(ck)

  // Subscribe from key
  ck.Subscribe("d")
  fmt.Println("asdasaa")
  v := <-ck.Receive
  fmt.Println("asdasaaasdasd")
  if v.Key() != "d" {
    t.Fatalf("Subscribed to wrong key")
  }

  // Put random keys
  ck.Put("a", "x")
  ck.Put("b", "x")
  ck.Put("c", "x")

  // Should receive changes to key=d
  ck.Put("d", "x")
  v = <-ck.Receive
  if v.PutValue() != "x" {
    t.Fatalf("Receive got the wrong value")
  }

  // New group join, old group leave
  mck.Join(gids[1], ha[1])
  mck.Leave(gids[0])

  // Unsubscribe from key
  ck.Unsubscribe("d")

  v = <-ck.Receive
  if v.Key() != "d" {
    t.Fatalf("Unsubscribed from wrong key")
  }

  // Close receive channel
  close(ck.Receive)

  // Souldn't receive anything now - will panic otherwise
  ck.Put("d", "x")

  time.Sleep(30 * time.Millisecond)

  fmt.Printf("  ... Passed\n")
}

func TestPubSubMove(t *testing.T) {
  smh, gids, ha, _, clean, _ := setup("pubsub-move", false, false)
  defer clean()

  fmt.Printf("Test: Pub/Sub Multiple Move ...\n")
  mck := shardmaster.MakeClerk(smh)
  for i := 0; i < len(gids); i++ {
    mck.Join(gids[i], ha[i])
  }

  ck := MakeClerk(smh)
  defer cleanupClerk(ck)

  ck.Subscribe("d")

  v := <-ck.Receive
  if v.Key() != "d" {
    t.Fatalf("Subscribed to wrong key")
  }

  for i := 0; i < shardmaster.NShards; i++ {
    val := string('0' + i)

    // Put random keys
    ck.Put("a", "aax")
    ck.Put("b", "bbx")
    ck.Put("c", "ccx")

    ck.Put("d", val)
    v = <-ck.Receive
    if v.PutValue() != val {
      t.Fatalf("Receive got the wrong value")
    }
    mck.Move(0, gids[rand.Int()%len(gids)])
  }

  // Unsubscribe from key
  ck.Unsubscribe("d")

  v = <-ck.Receive
  if v.Key() != "d" {
    t.Fatalf("Unsubscribed from wrong key")
  }

  close(ck.Receive)

  for i := 0; i < shardmaster.NShards; i++ {
    val := string('0' + i)
    ck.Put("d", val)
    mck.Move(0, gids[rand.Int()%len(gids)])
  }

  time.Sleep(30 * time.Millisecond)

  fmt.Printf("  ... Passed\n")
}

/*************************************************
****************PERSISTENCE TESTS*****************
*************************************************/

// func TestPersistenceDiskOkay(t *testing.T) {
//   smh, gids, ha, sa, clean, mbh := setup("persistencegood", false, false)
//   defer clean()

//   fmt.Printf("Test: Server recovers after failure and no disk loss...\n")

//   mck := shardmaster.MakeClerk(smh)
//   mck.Join(gids[0], ha[0])

//   ck := MakeClerk(smh)
//   defer cleanupClerk(ck)

//   ck.Put("a", "x")
//   v := ck.PutHash("a", "b")
//   if v != "x" {
//     t.Fatalf("Puthash got wrong value")
//   }
//   ov := NextValue("x", "b")
//   if ck.Get("a") != ov {
//     t.Fatalf("Get got wrong value")
//   }

//   keys := make([]string, 10)
//   vals := make([]string, len(keys))
//   for i := 0; i < len(keys); i++ {
//     keys[i] = strconv.Itoa(rand.Int())
//     vals[i] = strconv.Itoa(rand.Int())
//     ck.Put(keys[i], vals[i])
//   }

//   // are keys still there after kill and restart?
//   randomGroup := rand.Intn(len(sa))
//   randomSKV := rand.Intn(len(sa[randomGroup]))
//   sa[randomGroup][randomSKV].kill()
//   time.Sleep(1 * time.Second)
//   sa[randomGroup][randomSKV] = StartServer(gids[randomGroup], smh, ha[randomGroup], randomSKV, mbh)
//   time.Sleep(2 * time.Second)
//   for i := 0; i < len(keys); i++ {
//     v := sa[randomGroup][randomSKV].table[keys[i]]
//     if v.Value != vals[i] {
//       t.Fatalf("killed and restarted; wrong value; g=%v k=%v wanted=%v got=%v",
//         randomGroup, keys[i], vals[i], v)
//     }
//     // vals[i] = strconv.Itoa(rand.Int())
//     // ck.Put(keys[i], vals[i])
//   }

//   fmt.Printf("  ... Passed\n")
// }

// func TestPersistenceDiskLoss(t *testing.T) {
//   smh, gids, ha, _, clean, _ := setup("persistencebad", false, false)
//   defer clean()

//   fmt.Printf("Test: Server recovers after failure and total disk loss...\n")
// }

// func TestPersistenceSuddenDiskOkay(t *testing.T) {
//   smh, gids, ha, sa, clean, _ := setup("persistencesuddengood", false, true)
//   defer clean()

//   fmt.Printf("Test: Server recovers after sudden failure and no disk loss...\n")

//   mck := shardmaster.MakeClerk(smh)
//   mck.Join(gids[0], ha[0])

//   ck := MakeClerk(smh)
//   defer cleanupClerk(ck)

//   ck.Put("a", "x")
//   v := ck.PutHash("a", "b")
//   if v != "x" {
//     t.Fatalf("Puthash got wrong value")
//   }
//   ov := NextValue("x", "b")
//   if ck.Get("a") != ov {
//     t.Fatalf("Get got wrong value")
//   }

//   keys := make([]string, 10)
//   vals := make([]string, len(keys))
//   for i := 0; i < len(keys); i++ {
//     keys[i] = strconv.Itoa(rand.Int())
//     vals[i] = strconv.Itoa(rand.Int())
//     ck.Put(keys[i], vals[i])
//   }

//   // are keys still there after kill and restart?
//   randomGroup := rand.Intn(len(sa))
//   randomSKV := rand.Intn(len(sa[randomGroup]))
//   mb := sa[randomGroup][randomSKV].mb
//   sa[randomGroup][randomSKV].kill()
//   time.Sleep(1 * time.Second)
//   sa[randomGroup][randomSKV] = StartServer(gids[randomGroup], smh, ha[randomGroup], randomSKV, mb)
//   time.Sleep(2 * time.Second)
//   for i := 0; i < len(keys); i++ {
//     v := sa[randomGroup][randomSKV].table[keys[i]]
//     if v.Value != vals[i] {
//       t.Fatalf("killed and restarted; wrong value; g=%v k=%v wanted=%v got=%v",
//         randomGroup, keys[i], vals[i], v)
//     }
//     // vals[i] = strconv.Itoa(rand.Int())
//     // ck.Put(keys[i], vals[i])
//   }

//   fmt.Printf("  ... Passed\n")
// }

// func TestPersistenceSuddenDiskLoss(t *testing.T) {
//   smh, gids, ha, _, clean, _ := setup("persistencesuddenbad", false, true)
//   defer clean()

//   fmt.Printf("Test: Server recovers after sudden failure and total disk loss...\n")
// }
