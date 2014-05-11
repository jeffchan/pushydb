package pdb

import "testing"
import "fmt"
import leveldb "github.com/syndtr/goleveldb/leveldb"
import "encoding/gob"
import "os"
import "math/rand"

const DB_PATH = "/var/tmp/db/"

type Data struct {
  Field1 int64
  Field2 int
  Field3 string
}

func deleteDB(name string) {
  os.RemoveAll(DB_PATH+name)
}

func chk(t *testing.T, ok bool, val interface{}, true_val interface{}) {
  if !ok {
    t.Fatalf("Key not found!")
  }
  if val != true_val {
    t.Fatalf("Expected ", true_val, " got ", val)
  }
}

func TestBasic(t *testing.T) {
  dbname := "dummy.db"
  deleteDB(dbname)
  defer deleteDB(dbname)
  db, err := leveldb.OpenFile(DB_PATH+dbname, nil)
  if err != nil {
    t.Fatalf("Error opening db! %s\n", err)
  }
  pdb := StartDB(db)
  point := Data{1, 2, "hello"}
  gob.Register(Data{})
  gob.Register("ok")
  vals := []interface{}{"A", int(21), int64(22), true, point}
  for i, val := range vals {
    pdb.Put("paxos", i, "blah", val)
  }

  val0 := ""
  true_val0 := "A"
  ok := pdb.Get("paxos", 0, "blah", &val0)
  chk(t, ok, val0, true_val0)
  // XX A couple missing assertions(Blame go.)

  val1 := 0
  true_val1 := int(21)
  ok = pdb.Get("paxos", 1, "blah", &val1)
  chk(t, ok, val1, true_val1)

  val2 := int64(0)
  true_val2 := int64(22)
  ok = pdb.Get("paxos", 2, "blah", &val2)
  chk(t, ok, val2, true_val2)

  val3 := false
  true_val3 := true
  ok = pdb.Get("paxos", 3, "blah", &val3)
  chk(t, ok, val3, true_val3)

  val4 := Data{}
  true_val4 := point
  ok = pdb.Get("paxos", 4, "blah", &val4)
  chk(t, ok, val4, true_val4)

  if pdb.Get("paxos", -1, "blah", Data{}) != false || pdb.Get("paxos", 5, "blah", Data{}) != false {
    t.Fatalf("Expected key to not be found!")
  }

  db.Close()
  fmt.Println(" ...Passed")
}

func BenchmarkPut(b *testing.B) {
  fmt.Println("benchmarking...")
  dbname := "dummy.db"
  deleteDB(dbname)
  defer deleteDB(dbname)
  db, err := leveldb.OpenFile(DB_PATH+dbname, nil)
  if err != nil {
    b.Fatalf("Error opening db! %s\n", err)
  }
  pdb := StartDB(db)

  b.ResetTimer()
  for i:=0; i<b.N; i++ {
    big := make([]byte, 10000)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int()%26)
    }
    pdb.Put("tmp", big)
  }
}