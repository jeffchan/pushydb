package main

import "testing"
import "fmt"

func TestBasic(t *testing.T) {
  pdb := Make()
  var i int64
  i = 20
  pdb.Put("paxos", 20, "va", i)
  a := pdb.GetInt64("paxos", 20, "va")
  if a != i {
    t.Fatalf("Wrong value!")
  }
  fmt.Println("Passed!")
}
