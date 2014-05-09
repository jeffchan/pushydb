package main

import (
  "bytes"
  "encoding/binary"
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
// pdb.GetInt(key0, key1, ...) -> int
// pdb.GetInt64(key0, key1, ...) -> int64
// pdb.GetString(key0, key1, ...) -> string

// NOTE (the following is easily changed):
// All values should be int, int64, bool or string
// All keys should be string, int or int64

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
    case bool:
      buf := new(bytes.Buffer)
      err = binary.Write(buf, binary.LittleEndian, val)
      temp = buf.Bytes()
      id = 3
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

func booltobyte(b bool) byte {
  if b {
    return byte(1)
  }
  return byte(0)
}

func bytetobool(b byte) bool {
  if b == 0 {
    return false
  }
  if b == 1 {
    return true
  }
  log.Fatalf("Cannot convert to bool!")
  return true
}

func getBytesGob(val interface{}) []byte {
  buf := new(bytes.Buffer)
  enc := gob.NewEncoder(buf)
  err := enc.Encode(val)
  if err != nil {
    log.Fatal("Encode eror:", err)
  }
  return buf.Bytes()
}

// TODO clean up by doing everything with gob
func getBytes(val interface{}) []byte {
  buf := new(bytes.Buffer)
  switch val.(type) {
  case int:
    val = int64(val.(int))
  case string:
    return []byte(val.(string))
  case bool:
    return []byte{byte(1)}
  }
  err := binary.Write(buf, binary.LittleEndian, val)
  if err != nil {
    log.Fatal("Error converting to bytes", err)
  }
  return buf.Bytes()
}

func (pdb *PDB) PutGob(keyval ...interface{}) {
  if len(keyval) < 2 {
    log.Fatal("Need key value pair!")
  }
  key := keyval[:len(keyval) - 1]
  value := keyval[len(keyval) - 1]
  err := pdb.db.Put(getKeyBytes(key...), getBytesGob(value), &opt.WriteOptions{true})
  if err != nil {
    log.Fatal("Put to db failed!", err)
  }
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

func (pdb *PDB) GetInt64(key ...interface{}) (int64, bool) {
  var ans int64
  data, err := pdb.db.Get(getKeyBytes(key...), nil)
  if err == leveldb.ErrNotFound {
    return int64(0), false
  }
  buf := bytes.NewReader(data)
  err2 := binary.Read(buf, binary.LittleEndian, &ans)
  if err != nil || err2 != nil {
    log.Fatal("Get from db failed!", err, err2)
  }
  return ans, true
}

func (pdb *PDB) GetInt(key ...interface{}) (int, bool) {
  var ans int64
  data, err := pdb.db.Get(getKeyBytes(key...), nil)
  if err == leveldb.ErrNotFound {
    return 0, false
  }
  buf := bytes.NewReader(data)
  err2 := binary.Read(buf, binary.LittleEndian, &ans)
  if err != nil || err2 != nil {
    log.Fatal("Get from db failed!", err, err2)
  }
  return int(ans), true
}

func (pdb *PDB) GetBool(key ...interface{}) (bool, bool) {
  var ans byte
  data, err := pdb.db.Get(getKeyBytes(key...), nil)
  if err == leveldb.ErrNotFound {
    return false, false
  }
  buf := bytes.NewReader(data)
  err2 := binary.Read(buf, binary.LittleEndian, &ans)
  if err != nil || err2 != nil {
    log.Fatal("Get from db failed!", err, err2)
  }
  return bytetobool(ans), true
}

func (pdb *PDB) GetString(key ...interface{}) (string, bool) {
  data, err := pdb.db.Get(getKeyBytes(key...), nil)
  if err == leveldb.ErrNotFound {
    return "", false
  }
  if err != nil {
    log.Fatal("Get from db failed!", err)
  }
  return string(data), true
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
