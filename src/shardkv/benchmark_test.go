package shardkv

import "testing"
import "shardmaster"
import "runtime"
import "strconv"
import "time"
import "fmt"
import "math/rand"
import "messagebroker"
import "paxos"

func setupb(tag string, unreliable bool, sudden bool) ([]string, []int64, [][]string, [][]*ShardKV, func(), []string) {
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
      sa[i][j] = StartServer(gids[i], smh, ha[i], j, mbh)
      sa[i][j].unreliable = unreliable
    }
  }

  clean := func() { cleanup(sa); mcleanup(sma); mbcleanup(mba) }
  return smh, gids, ha, sa, clean, mbh
}

func doConcurrentB(b *testing.B, unreliable bool, subscribe bool) {
  smh, gids, ha, _, clean, _ := setupb("conc"+strconv.FormatBool(unreliable)+"-sub="+strconv.FormatBool(subscribe), unreliable, false)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  for i := 0; i < len(gids); i++ {
    mck.Join(gids[i], ha[i])
  }

  const npara = 11
  var ca [npara]chan bool
  clerks := make([]*Clerk, npara)
  for i := 0; i < npara; i++ {
    ca[i] = make(chan bool)
    go func(me int, clerks []*Clerk) {
      ok := true
      defer func() { ca[me] <- ok }()
      ck := MakeClerk(smh)
      clerks[me] = ck
      mymck := shardmaster.MakeClerk(smh)
      key := strconv.Itoa(me)
      last := ""

      if subscribe {
        ck.Subscribe(key)
        publish := <-ck.Receive
        if publish.Key() != key {
          ok = false
          b.Fatalf("Subscribed to wrong key")
        }
      }

      count := 3
      vals := make([]string, 0)
      for iters := 0; iters < count; iters++ {
        nv := strconv.Itoa(rand.Int())
        v := ck.PutHash(key, nv)
        if v != last {
          ok = false
          b.Fatalf("PutHash(%v) expected %v got %v\n", key, last, v)
        }
        last = NextValue(last, nv)
        v = ck.Get(key)
        if v != last {
          ok = false
          b.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
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
            ok = false
            b.Fatalf("Pub/sub received=%s, expected=%s", publish, vals[iters])
          }
        }

        ck.Unsubscribe(key)
        publish = <-ck.Receive
        if publish.Key() != key {
          ok = false
          b.Fatalf("Unsubscribed from wrong key")
        }
        close(ck.Receive)
      }

    }(i, clerks)
  }

  for i := 0; i < npara; i++ {
    x := <-ca[i]
    if x == false {
    b.Fatalf("something is wrong")
    }
  }

  time.Sleep(10 * time.Second)
  for i := 0; i < npara; i++ {
    cleanupClerk(clerks[i])
  }
}

func BenchmarkConcurrent(b *testing.B) {
  fmt.Printf("\nMULTIPAXOS=%b", paxos.MultiPaxosOn)

  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    doConcurrentB(b, false, true)
  }
}

func BenchmarkConcurrentUnreliable(b *testing.B) {
  fmt.Printf("\nMULTIPAXOS=%b", paxos.MultiPaxosOn)

  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    doConcurrentB(b, true, true)
  }
}
