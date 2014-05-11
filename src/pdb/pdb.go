package pdb

import (
  "bytes"
  "encoding/gob"
  leveldb "github.com/syndtr/goleveldb/leveldb"
  opt "github.com/syndtr/goleveldb/leveldb/opt"
  "log"
  // "os"
  "fmt"
)

var _ = fmt.Println

// Wrapper around levelDB
// Interface:
// Make() -> PDB object
// pdb.Delete()
// Recover() -> PDB object
// pdb.Put(key0, key1, ..., value)
// pdb.Get(key0, key1, ..., pointer to value) -> bool (false if key not found)
// You must gob.Register(..) any types you use as values.

// TODO
// pdb.CLose() (closes the database) throws an error
// indicate goroutines aren't being cleaned up properly
// uh oh!

type PDB struct {
  DB     leveldb.DB
  prefix string
}

// LevelDB commands
// data, err := db.Get([]byte("key"), nil)
// err = db.Put([]byte("key"), []byte("value"), nil)
// err = db.Delete([]byte("key"), nil)

func (pdb *PDB) getKeyBytes(vals ...interface{}) []byte {
  ans := make([]byte, 0)
  ans = append(ans, getBytes(pdb.prefix)...)
  for _, val := range vals {
    ans = append(ans, getBytes(val)...)
  }
  return ans
}

func getBytes(val interface{}) []byte {
  buf := new(bytes.Buffer)
  enc := gob.NewEncoder(buf)
  err := enc.Encode(val)
  if err != nil {
    log.Fatal("Encode eror:", err)
  }
  return buf.Bytes()
}

func (pdb *PDB) Put(keyval ...interface{}) {
  if len(keyval) < 2 {
    log.Fatal("Need key value pair!")
  }
  key := keyval[:len(keyval)-1]
  value := keyval[len(keyval)-1]
  err := pdb.DB.Put(pdb.getKeyBytes(key...), getBytes(value), &opt.WriteOptions{Sync: true})
  if err != nil {
    log.Fatal("Put to db failed:", err)
  }
}

func (pdb *PDB) Get(keyval ...interface{}) bool {
  if len(keyval) < 2 {
    log.Fatal("Need key value pair!")
  }
  key := keyval[:len(keyval)-1]
  value := keyval[len(keyval)-1]
  data, err := pdb.DB.Get(pdb.getKeyBytes(key...), nil)
  if err == leveldb.ErrNotFound {
    return false
  }
  dec := gob.NewDecoder(bytes.NewBuffer(data))
  err2 := dec.Decode(value)
  if err != nil || err2 != nil {
    log.Fatal("Get from db failed:", err, err2)
  }
  return true
}

func StartDB(database *leveldb.DB, prefix string) *PDB {
  pdb := &PDB{}
  pdb.DB = *database
  pdb.prefix = prefix
  return pdb
}
