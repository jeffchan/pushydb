package main

import "testing"
import "fmt"

func TestBasic(t *testing.T) {
  pdb := Make()
  var i int64
  i = 20
  pdb.Put("key", i)
  a := pdb.GetInt64("key")
  if a != i {
    t.Fatalf("Wrong value!")
  }
  fmt.Println("Passed!")
}
