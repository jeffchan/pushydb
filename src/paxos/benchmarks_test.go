package paxos

// import "testing"
// import "runtime"
// import "strconv"
// import "os"
// import "time"
// import "fmt"
// import "math/rand"

// func portb(tag string, host int) string {
//   s := "/var/tmp/824-"
//   s += strconv.Itoa(os.Getuid()) + "/"
//   os.Mkdir(s, 0777)
//   s += "px-"
//   s += strconv.Itoa(os.Getpid()) + "-"
//   s += tag + "-"
//   s += strconv.Itoa(host)
//   return s
// }

// func ndecidedb(b *testing.B, pxa []*Paxos, seq int) int {
//   count := 0
//   var v interface{}
//   for i := 0; i < len(pxa); i++ {
//     if pxa[i] != nil {
//       decided, v1 := pxa[i].Status(seq)
//       if decided {
//         if count > 0 && v != v1 {
//           b.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
//             seq, i, v, v1)
//         }
//         count++
//         v = v1
//       }
//     }
//   }
//   return count
// }

// func waitnb(b *testing.B, pxa []*Paxos, seq int, wanted int) {
//   to := 10 * time.Millisecond
//   for iters := 0; iters < 30; iters++ {
//     if ndecidedb(b, pxa, seq) >= wanted {
//       break
//     }
//     time.Sleep(to)
//     if to < time.Second {
//       to *= 2
//     }
//   }
//   nd := ndecidedb(b, pxa, seq)
//   if nd < wanted {
//     b.Fatalf("too few decided; seq=%v ndecidedb=%v wanted=%v", seq, nd, wanted)
//   }
// }

// func waitmajorityb(b *testing.B, pxa []*Paxos, seq int) {
//   waitnb(b, pxa, seq, (len(pxa)/2)+1)
// }

// func checkmaxb(b *testing.B, pxa []*Paxos, seq int, max int) {
//   time.Sleep(3 * time.Second)
//   nd := ndecidedb(b, pxa, seq)
//   if nd > max {
//     b.Fatalf("too many decided; seq=%v ndecidedb=%v max=%v", seq, nd, max)
//   }
// }

// func cleanupb(pxa []*Paxos) {
//   for i := 0; i < len(pxa); i++ {
//     if pxa[i] != nil {
//       pxa[i].Kill()
//     }
//   }
// }

// func noTestSpeedb(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   const npaxos = 3
//   var pxa []*Paxos = make([]*Paxos, npaxos)
//   var pxh []string = make([]string, npaxos)
//   defer cleanupb(pxa)

//   for i := 0; i < npaxos; i++ {
//     pxh[i] = portb("time", i)
//   }
//   for i := 0; i < npaxos; i++ {
//     pxa[i] = Make(pxh, i, nil)
//   }

//   t0 := time.Now()

//   for i := 0; i < 20; i++ {
//     pxa[0].Start(i, "x")
//     waitnb(b, pxa, i, npaxos)
//   }

//   d := time.Since(t0)
//   fmt.Printf("20 agreements %v seconds\n", d.Seconds())
// }

// func BenchmarkBasic(b *testing.B) {
//   runtime.GOMAXPROCS(4)
//   fmt.Printf("\nMULTIPAXOS=%b", MultiPaxosOn)
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 3
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("basic", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//     }

//     // fmt.Printf("Test: Single proposer ...\n")

//     pxa[0].Start(0, "hello")
//     waitnb(b, pxa, 0, npaxos)

//     // fmt.Printf("  ... Passed\n")

//     // fmt.Printf("Test: Many proposers, same value ...\n")

//     for i := 0; i < npaxos; i++ {
//       pxa[i].Start(1, 77)
//     }
//     waitnb(b, pxa, 1, npaxos)

//     // fmt.Printf("  ... Passed\n")

//     // fmt.Printf("Test: Many proposers, different values ...\n")

//     pxa[0].Start(2, 100)
//     pxa[1].Start(2, 101)
//     pxa[2].Start(2, 102)
//     waitnb(b, pxa, 2, npaxos)

//     // fmt.Printf("  ... Passed\n")

//     // fmt.Printf("Test: Out-of-order instances ...\n")

//     pxa[0].Start(7, 700)
//     pxa[0].Start(6, 600)
//     pxa[1].Start(5, 500)
//     waitnb(b, pxa, 7, npaxos)
//     pxa[0].Start(4, 400)
//     pxa[1].Start(3, 300)
//     waitnb(b, pxa, 6, npaxos)
//     waitnb(b, pxa, 5, npaxos)
//     waitnb(b, pxa, 4, npaxos)
//     waitnb(b, pxa, 3, npaxos)

//     if pxa[0].Max() != 7 {
//       b.Fatalf("wrong Max()")
//     }

//     // fmt.Printf("  ... Passed\n")
//   }
// }

// func BenchmarkDeaf(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 5
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("deaf", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//     }

//     // fmt.Printf("Test: Deaf proposer ...\n")

//     pxa[0].Start(0, "hello")
//     waitnb(b, pxa, 0, npaxos)

//     os.Remove(pxh[0])
//     os.Remove(pxh[npaxos-1])

//     pxa[1].Start(1, "goodbye")
//     waitmajorityb(b, pxa, 1)
//     time.Sleep(1 * time.Second)
//     if ndecidedb(b, pxa, 1) != npaxos-2 {
//       b.Fatalf("a deaf peer heard about a decision")
//     }

//     pxa[0].Start(1, "xxx")
//     waitnb(b, pxa, 1, npaxos-1)
//     time.Sleep(1 * time.Second)
//     if ndecidedb(b, pxa, 1) != npaxos-1 {
//       b.Fatalf("a deaf peer heard about a decision")
//     }

//     pxa[npaxos-1].Start(1, "yyy")
//     waitnb(b, pxa, 1, npaxos)
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// func BenchmarkForget(b *testing.B) {
//   runtime.GOMAXPROCS(4)
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 6
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("gc", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//     }

//     // fmt.Printf("Test: Forgetting ...\n")

//     // initial Min() correct?
//     for i := 0; i < npaxos; i++ {
//       m := pxa[i].Min()
//       if m > 0 {
//         b.Fatalf("wrong initial Min() %v", m)
//       }
//     }

//     pxa[0].Start(0, "00")
//     pxa[1].Start(1, "11")
//     pxa[2].Start(2, "22")
//     pxa[0].Start(6, "66")
//     pxa[1].Start(7, "77")

//     waitnb(b, pxa, 0, npaxos)

//     // Min() correct?
//     for i := 0; i < npaxos; i++ {
//       m := pxa[i].Min()
//       if m != 0 {
//         b.Fatalf("wrong Min() %v; expected 0", m)
//       }
//     }

//     waitnb(b, pxa, 1, npaxos)

//     // Min() correct?
//     for i := 0; i < npaxos; i++ {
//       m := pxa[i].Min()
//       if m != 0 {
//         b.Fatalf("wrong Min() %v; expected 0", m)
//       }
//     }

//     // everyone Done() -> Min() changes?
//     for i := 0; i < npaxos; i++ {
//       pxa[i].Done(0)
//     }
//     for i := 1; i < npaxos; i++ {
//       pxa[i].Done(1)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i].Start(8+i, "xx")
//     }
//     allok := false
//     for iters := 0; iters < 12; iters++ {
//       allok = true
//       for i := 0; i < npaxos; i++ {
//         s := pxa[i].Min()
//         if s != 1 {
//           allok = false
//         }
//       }
//       if allok {
//         break
//       }
//       time.Sleep(1 * time.Second)
//     }
//     if allok != true {
//       b.Fatalf("Min() did not advance after Done()")
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// func BenchmarkManyForget(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 3
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("manygc", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//       pxa[i].unreliable = true
//     }

//     // fmt.Printf("Test: Lots of forgetting ...\n")

//     const maxseq = 20
//     done := false

//     go func() {
//       na := rand.Perm(maxseq)
//       for i := 0; i < len(na); i++ {
//         seq := na[i]
//         j := (rand.Int() % npaxos)
//         v := rand.Int()
//         pxa[j].Start(seq, v)
//         runtime.Gosched()
//       }
//     }()

//     go func() {
//       for done == false {
//         seq := (rand.Int() % maxseq)
//         i := (rand.Int() % npaxos)
//         if seq >= pxa[i].Min() {
//           decided, _ := pxa[i].Status(seq)
//           if decided {
//             pxa[i].Done(seq)
//           }
//         }
//         runtime.Gosched()
//       }
//     }()

//     time.Sleep(5 * time.Second)
//     done = true
//     for i := 0; i < npaxos; i++ {
//       pxa[i].unreliable = false
//     }
//     time.Sleep(2 * time.Second)

//     for seq := 0; seq < maxseq; seq++ {
//       for i := 0; i < npaxos; i++ {
//         if seq >= pxa[i].Min() {
//           pxa[i].Status(seq)
//         }
//       }
//     }
//   }

//   // fmt.Printf("  ... Passed\n")
// }

// //
// // does paxos forgetting actually free the memory?
// //
// func BenchmarkForgetMem(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   // fmt.Printf("Test: Paxos frees forgotten instance memory ...\n")
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 3
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("gcmem", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//     }

//     pxa[0].Start(0, "x")
//     waitnb(b, pxa, 0, npaxos)

//     runtime.GC()
//     var m0 runtime.MemStats
//     runtime.ReadMemStats(&m0)
//     // m0.Alloc about a megabyte

//     for i := 1; i <= 10; i++ {
//       big := make([]byte, 1000000)
//       for j := 0; j < len(big); j++ {
//         big[j] = byte('a' + rand.Int()%26)
//       }
//       pxa[0].Start(i, string(big))
//       waitnb(b, pxa, i, npaxos)
//     }

//     runtime.GC()
//     var m1 runtime.MemStats
//     runtime.ReadMemStats(&m1)
//     // m1.Alloc about 90 megabytes

//     for i := 0; i < npaxos; i++ {
//       pxa[i].Done(10)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i].Start(11+i, "z")
//     }
//     time.Sleep(3 * time.Second)
//     for i := 0; i < npaxos; i++ {
//       if pxa[i].Min() != 11 {
//         b.Fatalf("expected Min() %v, got %v\n", 11, pxa[i].Min())
//       }
//     }

//     runtime.GC()
//     var m2 runtime.MemStats
//     runtime.ReadMemStats(&m2)
//     // m2.Alloc about 10 megabytes

//     if m2.Alloc > (m1.Alloc / 2) {
//       b.Fatalf("memory use did not shrink enough")
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// func BenchmarkRPCCount(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   // fmt.Printf("Test: RPC counts aren't too high ...\n")
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 3
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("count", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//     }

//     ninst1 := 5
//     seq := 0
//     for i := 0; i < ninst1; i++ {
//       pxa[0].Start(seq, "x")
//       waitnb(b, pxa, seq, npaxos)
//       seq++
//     }

//     time.Sleep(2 * time.Second)

//     total1 := 0
//     for j := 0; j < npaxos; j++ {
//       total1 += pxa[j].rpcCount
//     }

//     // per agreement:
//     // 3 prepares
//     // 3 accepts
//     // 3 decides
//     expected1 := ninst1 * npaxos * npaxos
//     if total1 > expected1 {
//       b.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
//         ninst1, total1, expected1)
//     }

//     ninst2 := 5
//     for i := 0; i < ninst2; i++ {
//       for j := 0; j < npaxos; j++ {
//         go pxa[j].Start(seq, j+(i*10))
//       }
//       waitnb(b, pxa, seq, npaxos)
//       seq++
//     }

//     time.Sleep(2 * time.Second)

//     total2 := 0
//     for j := 0; j < npaxos; j++ {
//       total2 += pxa[j].rpcCount
//     }
//     total2 -= total1

//     // worst case per agreement:
//     // Proposer 1: 3 prep, 3 acc, 3 decides.
//     // Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
//     // Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
//     expected2 := ninst2 * npaxos * 15
//     if total2 > expected2 {
//       b.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
//         ninst2, total2, expected2)
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// //
// // many agreements (without failures)
// //
// func BenchmarkMany(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   // fmt.Printf("Test: Many instances ...\n")
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 3
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("many", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//       pxa[i].Start(0, 0)
//     }

//     const ninst = 50
//     for seq := 1; seq < ninst; seq++ {
//       // only 5 active instances, to limit the
//       // number of file descriptors.
//       for seq >= 5 && ndecidedb(b, pxa, seq-5) < npaxos {
//         time.Sleep(20 * time.Millisecond)
//       }
//       for i := 0; i < npaxos; i++ {
//         pxa[i].Start(seq, (seq*10)+i)
//       }
//     }

//     for {
//       done := true
//       for seq := 1; seq < ninst; seq++ {
//         if ndecidedb(b, pxa, seq) < npaxos {
//           done = false
//         }
//       }
//       if done {
//         break
//       }
//       time.Sleep(100 * time.Millisecond)
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// //
// // a peer starts up, with proposal, after others decide.
// // then another peer starts, without a proposal.
// //
// func BenchmarkOld(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   // fmt.Printf("Test: Minority proposal ignored ...\n")
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 5
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("old", i)
//     }

//     pxa[1] = Make(pxh, 1, nil)
//     pxa[2] = Make(pxh, 2, nil)
//     pxa[3] = Make(pxh, 3, nil)
//     pxa[1].Start(1, 111)

//     waitmajorityb(b, pxa, 1)

//     pxa[0] = Make(pxh, 0, nil)
//     pxa[0].Start(1, 222)

//     waitnb(b, pxa, 1, 4)

//     if false {
//       pxa[4] = Make(pxh, 4, nil)
//       waitnb(b, pxa, 1, npaxos)
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// //
// // many agreements, with unreliable RPC
// //
// func BenchmarkManyUnreliable(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   // fmt.Printf("Test: Many instances, unreliable RPC ...\n")
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     const npaxos = 3
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     var pxh []string = make([]string, npaxos)
//     defer cleanupb(pxa)

//     for i := 0; i < npaxos; i++ {
//       pxh[i] = portb("manyun", i)
//     }
//     for i := 0; i < npaxos; i++ {
//       pxa[i] = Make(pxh, i, nil)
//       pxa[i].unreliable = true
//       pxa[i].Start(0, 0)
//     }

//     const ninst = 50
//     for seq := 1; seq < ninst; seq++ {
//       // only 3 active instances, to limit the
//       // number of file descriptors.
//       for seq >= 3 && ndecidedb(b, pxa, seq-3) < npaxos {
//         time.Sleep(20 * time.Millisecond)
//       }
//       for i := 0; i < npaxos; i++ {
//         pxa[i].Start(seq, (seq*10)+i)
//       }
//     }

//     for {
//       done := true
//       for seq := 1; seq < ninst; seq++ {
//         if ndecidedb(b, pxa, seq) < npaxos {
//           done = false
//         }
//       }
//       if done {
//         break
//       }
//       time.Sleep(100 * time.Millisecond)
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// func ppb(tag string, src int, dst int) string {
//   s := "/var/tmp/824-"
//   s += strconv.Itoa(os.Getuid()) + "/"
//   s += "px-" + tag + "-"
//   s += strconv.Itoa(os.Getpid()) + "-"
//   s += strconv.Itoa(src) + "-"
//   s += strconv.Itoa(dst)
//   return s
// }

// func cleanppb(tag string, n int) {
//   for i := 0; i < n; i++ {
//     for j := 0; j < n; j++ {
//       ij := ppb(tag, i, j)
//       os.Remove(ij)
//     }
//   }
// }

// func partb(b *testing.B, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
//   cleanppb(tag, npaxos)

//   pa := [][]int{p1, p2, p3}
//   for pi := 0; pi < len(pa); pi++ {
//     p := pa[pi]
//     for i := 0; i < len(p); i++ {
//       for j := 0; j < len(p); j++ {
//         ij := ppb(tag, p[i], p[j])
//         pj := portb(tag, p[j])
//         err := os.Link(pj, ij)
//         if err != nil {
//           // one reason this link can fail is if the
//           // corresponding Paxos peer has prematurely quit and
//           // deleted its socket file (e.g., called px.Kill()).
//           b.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
//         }
//       }
//     }
//   }
// }

// func BenchmarkPartition(b *testing.B) {
//   runtime.GOMAXPROCS(4)
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     tag := "partition"
//     const npaxos = 5
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     defer cleanupb(pxa)
//     defer cleanppb(tag, npaxos)

//     for i := 0; i < npaxos; i++ {
//       var pxh []string = make([]string, npaxos)
//       for j := 0; j < npaxos; j++ {
//         if j == i {
//           pxh[j] = portb(tag, i)
//         } else {
//           pxh[j] = ppb(tag, i, j)
//         }
//       }
//       pxa[i] = Make(pxh, i, nil)
//     }
//     defer partb(b, tag, npaxos, []int{}, []int{}, []int{})

//     seq := 0

//     // fmt.Printf("Test: No decision if partitioned ...\n")

//     partb(b, tag, npaxos, []int{0, 2}, []int{1, 3}, []int{4})
//     pxa[1].Start(seq, 111)
//     checkmaxb(b, pxa, seq, 0)

//     // fmt.Printf("  ... Passed\n")

//     // fmt.Printf("Test: Decision in majority partition ...\n")

//     partb(b, tag, npaxos, []int{0}, []int{1, 2, 3}, []int{4})
//     time.Sleep(2 * time.Second)
//     waitmajorityb(b, pxa, seq)

//     // fmt.Printf("  ... Passed\n")

//     // fmt.Printf("Test: All agree after full heal ...\n")

//     pxa[0].Start(seq, 1000) // poke them
//     pxa[4].Start(seq, 1004)
//     partb(b, tag, npaxos, []int{0, 1, 2, 3, 4}, []int{}, []int{})

//     waitnb(b, pxa, seq, npaxos)

//     // fmt.Printf("  ... Passed\n")

//     // fmt.Printf("Test: One peer switches partitions ...\n")

//     for iters := 0; iters < 20; iters++ {
//       seq++

//       partb(b, tag, npaxos, []int{0, 1, 2}, []int{3, 4}, []int{})
//       pxa[0].Start(seq, seq*10)
//       pxa[3].Start(seq, (seq*10)+1)
//       waitmajorityb(b, pxa, seq)
//       if ndecidedb(b, pxa, seq) > 3 {
//         b.Fatalf("too many decided")
//       }

//       partb(b, tag, npaxos, []int{0, 1}, []int{2, 3, 4}, []int{})
//       waitnb(b, pxa, seq, npaxos)
//     }

//     // fmt.Printf("  ... Passed\n")

//     // fmt.Printf("Test: One peer switches partitions, unreliable ...\n")

//     for iters := 0; iters < 20; iters++ {
//       seq++

//       for i := 0; i < npaxos; i++ {
//         pxa[i].unreliable = true
//       }

//       partb(b, tag, npaxos, []int{0, 1, 2}, []int{3, 4}, []int{})
//       for i := 0; i < npaxos; i++ {
//         pxa[i].Start(seq, (seq*10)+i)
//       }
//       waitnb(b, pxa, seq, 3)
//       if ndecidedb(b, pxa, seq) > 3 {
//         b.Fatalf("too many decided")
//       }

//       partb(b, tag, npaxos, []int{0, 1}, []int{2, 3, 4}, []int{})

//       for i := 0; i < npaxos; i++ {
//         pxa[i].unreliable = false
//       }

//       waitnb(b, pxa, seq, 5)
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }

// func BenchmarkLots(b *testing.B) {
//   runtime.GOMAXPROCS(4)

//   // fmt.Printf("Test: Many requests, changing partitions ...\n")
//   b.ResetTimer()
//   for i := 0; i < b.N; i++ {
//     tag := "lots"
//     const npaxos = 5
//     var pxa []*Paxos = make([]*Paxos, npaxos)
//     defer cleanupb(pxa)
//     defer cleanppb(tag, npaxos)

//     for i := 0; i < npaxos; i++ {
//       var pxh []string = make([]string, npaxos)
//       for j := 0; j < npaxos; j++ {
//         if j == i {
//           pxh[j] = portb(tag, i)
//         } else {
//           pxh[j] = ppb(tag, i, j)
//         }
//       }
//       pxa[i] = Make(pxh, i, nil)
//       pxa[i].unreliable = true
//     }
//     defer partb(b, tag, npaxos, []int{}, []int{}, []int{})

//     done := false

//     // re-partition periodically
//     ch1 := make(chan bool)
//     go func() {
//       defer func() { ch1 <- true }()
//       for done == false {
//         var a [npaxos]int
//         for i := 0; i < npaxos; i++ {
//           a[i] = (rand.Int() % 3)
//         }
//         pa := make([][]int, 3)
//         for i := 0; i < 3; i++ {
//           pa[i] = make([]int, 0)
//           for j := 0; j < npaxos; j++ {
//             if a[j] == i {
//               pa[i] = append(pa[i], j)
//             }
//           }
//         }
//         partb(b, tag, npaxos, pa[0], pa[1], pa[2])
//         time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
//       }
//     }()

//     seq := 0

//     // periodically start a new instance
//     ch2 := make(chan bool)
//     go func() {
//       defer func() { ch2 <- true }()
//       for done == false {
//         // how many instances are in progress?
//         nd := 0
//         for i := 0; i < seq; i++ {
//           if ndecidedb(b, pxa, i) == npaxos {
//             nd++
//           }
//         }
//         if seq-nd < 10 {
//           for i := 0; i < npaxos; i++ {
//             pxa[i].Start(seq, rand.Int()%10)
//           }
//           seq++
//         }
//         time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
//       }
//     }()

//     // periodically check that decisions are consistent
//     ch3 := make(chan bool)
//     go func() {
//       defer func() { ch3 <- true }()
//       for done == false {
//         for i := 0; i < seq; i++ {
//           ndecidedb(b, pxa, i)
//         }
//         time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
//       }
//     }()

//     time.Sleep(20 * time.Second)
//     done = true
//     <-ch1
//     <-ch2
//     <-ch3

//     // repair, then check that all instances decided.
//     for i := 0; i < npaxos; i++ {
//       pxa[i].unreliable = false
//     }
//     partb(b, tag, npaxos, []int{0, 1, 2, 3, 4}, []int{}, []int{})
//     time.Sleep(5 * time.Second)

//     for i := 0; i < seq; i++ {
//       waitmajorityb(b, pxa, i)
//     }
//   }
//   // fmt.Printf("  ... Passed\n")
// }
