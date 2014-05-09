package main

import "testing"
import "fmt"

type Data struct {
  Field1 int64
  Field2 int
  Field3 string
}

func first(args ...interface{})interface{} {
    return args[0]
}

func second(args ...interface{})interface{} {
    return args[1]
}

func TestBasic(t *testing.T) {
  Delete()
  pdb := Make()
  point := Data{1, 2, "hello"}
  vals := []interface{}{"A", int(21), int64(22), true, point}
  for i, val := range vals {
    pdb.PutGob("paxos", i, "blah", val)
  }
  if first(pdb.GetGob("paxos", 4, "blah") != point) {
    t.Falatf("Wrong struct!")
  }
  if first(pdb.GetString("paxos", 0, "blah")) != "A" {
    t.Fatalf("Wrong string!")
  }
  if first(pdb.GetInt("paxos", 1, "blah")) != int(21) {
    t.Fatalf("Wrong   int!")
  }
  if first(pdb.GetInt64("paxos", 2, "blah")) != int64(22) {
    t.Fatalf("Wrong int64!")
  }
  if first(pdb.GetBool("paxos", 3, "blah")) != true {
    t.Fatalf("Wrong bool!")
  }
  if second(pdb.GetBool("paxos", 5, "blah")) != false {
    t.Fatalf("Not found error not working in GetBool!")
  }
  if second(pdb.GetInt("paxos", 5, "blah")) != false {
    t.Fatalf("Not found error not working in GetInt!")
  }
  if second(pdb.GetString("paxos", 5, "blah")) != false {
    t.Fatalf("Not found error not working in GetString!")
  }
  if second(pdb.GetInt64("paxos", 5, "blah")) != false {
    t.Fatalf("Not found error not working in GetInt64!")
  }
  fmt.Println("Passed!")
  Delete()
}
