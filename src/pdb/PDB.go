package main

import (
  "bytes"
  leveldb "github.com/syndtr/goleveldb/leveldb"
  opt "github.com/syndtr/goleveldb/leveldb/opt"
  "log"
  "os"
  "encoding/gob"
)

const (
  DB_PATH = "data"
)

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
  db leveldb.DB
}

// LevelDB commands
// data, err := db.Get([]byte("key"), nil)
// err = db.Put([]byte("key"), []byte("value"), nil)
// err = db.Delete([]byte("key"), nil)

func getKeyBytes(vals ...interface{}) []byte {
  ans := make([]byte, 0)
  for _, val := range(vals) {
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
  key := keyval[:len(keyval) - 1]
  value := keyval[len(keyval) - 1]
  err := pdb.db.Put(getKeyBytes(key...), getBytes(value), &opt.WriteOptions{true})
  if err != nil {
    log.Fatal("Put to db failed:", err)
  }
}

func (pdb *PDB) Get(keyval ...interface{}) bool {
  if len(keyval) < 2 {
    log.Fatal("Need key value pair!")
  }
  key := keyval[:len(keyval) - 1]
  value := keyval[len(keyval) - 1]
  data, err := pdb.db.Get(getKeyBytes(key...), nil)
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

func Make() *PDB {
  pdb := &PDB{}
  pdb.open()
  return pdb
}

func (pdb *PDB) open() {
  db, err := leveldb.OpenFile(DB_PATH, nil)
  pdb.db = *db
  if err != nil {
    log.Fatal("Error opening db!", err)
  }
}

func (pdb *PDB) Close() {
  pdb.db.Close()
} //doesn't work!

func Delete() {
  os.RemoveAll(DB_PATH) //careful!
}

func (pdb *PDB) Recover() {
  db, err := leveldb.RecoverFile(DB_PATH, nil)
  pdb.db = *db
  if err != nil {
    log.Fatal("Error recovering db!", err)
  }
}