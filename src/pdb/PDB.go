package main

import (
  leveldb "github.com/syndtr/goleveldb/leveldb"
  opt "github.com/syndtr/goleveldb/leveldb/opt"
  "bytes"
  "encoding/binary"
  "log"
)

const (
  DB_PATH = "data"
)

// Wrapper around levelDB


type PDB struct {
  db leveldb.DB

}

// LevelDB commands
// data, err := db.Get([]byte("key"), nil)
// err = db.Put([]byte("key"), []byte("value"), nil)
// err = db.Delete([]byte("key"), nil)

func (pdb *PDB) Put(var_name string, value interface{}) {
  buf := new(bytes.Buffer)
  err := binary.Write(buf, binary.LittleEndian, value)
  err2 := pdb.db.Put([]byte(var_name), buf.Bytes(), &opt.WriteOptions{true})
  if err != nil || err2 != nil {
    log.Fatal("Put to db failed!", err, err2)
  }
}

func (pdb *PDB) GetInt64(var_name string) int64 {
  var ans int64
  data, err := pdb.db.Get([]byte(var_name), nil) 
  buf := bytes.NewReader(data)
  err2 := binary.Read(buf, binary.LittleEndian, &ans)
  if err != nil || err2 != nil {
    log.Fatal("Get from db failed!", err, err2)
  }
  return ans
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
}

func (pdb *PDB) Recover() {
  db, err := leveldb.RecoverFile(DB_PATH, nil)
  pdb.db = *db
  if err != nil {
    log.Fatal("Error recovering db!", err)
  }
}