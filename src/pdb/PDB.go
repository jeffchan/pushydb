package main

import (
  "bytes"
  "encoding/binary"
  leveldb "github.com/syndtr/goleveldb/leveldb"
  opt "github.com/syndtr/goleveldb/leveldb/opt"
  "log"
)

const (
  DB_PATH = "data"
)

// Wrapper around levelDB
// Interface:
// Make() -> PDB object
// pdb.Close()
// Recover() -> PDB object
// pdb.Put(key0, key1, ..., value)
// pdb.GetInt(key0, key1, ...) -> int
// pdb.GetInt64(key0, key1, ...) -> int64

// NOTE (the following is easily changed):
// All values should be int or int64
// All key should be string, int or int64

type PDB struct {
  db leveldb.DB
}

// LevelDB commands
// data, err := db.Get([]byte("key"), nil)
// err = db.Put([]byte("key"), []byte("value"), nil)
// err = db.Delete([]byte("key"), nil)

// Represents int/int64/string objects as byte array
func getKeyBytes(vals ...interface{}) []byte {
  ans := make([]byte, 0)
  for _, val := range(vals) {
    var temp []byte
    var err interface{}
    var id byte
    err = nil
    switch val.(type) {
    case int:
      val = int64(val.(int))
      buf := new(bytes.Buffer)
      err = binary.Write(buf, binary.LittleEndian, val)
      temp = buf.Bytes()
      id = 0
    case string:
      // string isn't fixed width, so we can't use binary.Write
      temp = []byte(val.(string))
      id = 1
    case int64:
      buf := new(bytes.Buffer)
      err = binary.Write(buf, binary.LittleEndian, val)
      temp = buf.Bytes()
      id = 2
    default:
      log.Fatal("Type not supported yet!")
    }
    if len(temp) > 255 || err != nil {
      log.Fatal("Conversion to byte array failed", len(temp), err)
    }
    ans = append(ans, byte(len(temp)), id)
    ans = append(ans, temp...)
  }
  return ans
}

func getBytes(val interface{}) []byte {
  buf := new(bytes.Buffer)
  switch val.(type) {
  case int:
    val = int64(val.(int))
  }
  err := binary.Write(buf, binary.LittleEndian, val)
  if err != nil {
    log.Fatal("Error converting to bytes", err)
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
    log.Fatal("Put to db failed!", err)
  }
}

func (pdb *PDB) GetInt64(key ...interface{}) int64 {
  var ans int64
  data, err := pdb.db.Get(getKeyBytes(key...), nil)
  buf := bytes.NewReader(data)
  err2 := binary.Read(buf, binary.LittleEndian, &ans)
  if err != nil || err2 != nil {
    log.Fatal("Get from db failed!", err, err2)
  }
  return ans
}

func (pdb *PDB) GetInt(key ...interface{}) int {
  var ans int64
  data, err := pdb.db.Get(getKeyBytes(key...), nil)
  buf := bytes.NewReader(data)
  err2 := binary.Read(buf, binary.LittleEndian, &ans)
  if err != nil || err2 != nil {
    log.Fatal("Get from db failed!", err, err2)
  }
  return int(ans)
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
