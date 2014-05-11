package pdb

import "testing"
import "fmt"
import "encoding/gob"

type Data struct {
  Field1 int64
  Field2 int
  Field3 string
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

  Delete()
  pdb := Make()
  point := Data{1, 2, "hello"}
  gob.Register(Data{})
  gob.Register("ok")
  vals := []interface{}{"A", int(21), int64(22), true, point}
  for i, val := range vals {
    pdb.Put("paxos", i, "blah", val)
  }
  // Amazingly, go doesn't let you write a for loop for this. Amazing.
  val := ""
  true_val := "A"
  ok := pdb.Get("paxos", 0, "blah", &val)
  chk(t, ok, val, true_val)
  // XX A couple missing assertions(Blame go.)
  val4 := Data{}
  true_val4 := point
  ok = pdb.Get("paxos", 4, "blah", &val4)
  chk(t, ok, val4, true_val4)
  if pdb.Get("paxos", -1, "blah", Data{}) != false {
    t.Fatalf("Expected key to not be found!")
  }
  fmt.Println("Passed!")
  Delete()
}
