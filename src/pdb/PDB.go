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

type PDB struct {
  db leveldb.DB
}

// LevelDB commands
// data, err := db.Get([]byte("key"), nil)
// err = db.Put([]byte("key"), []byte("value"), nil)
// err = db.Delete([]byte("key"), nil)


// Mashes objects into a byte array
package main

import "fmt"
import "bytes"
import "encoding/binary"
import "log"

// One to one function from a bunch of string/int/fixed size objects to
// byte array :)
func getBytes(vals ...interface{}) []byte {
  ans := make([]byte, 0)
  for _, val := range(vals) {
    var temp []byte
    var err interface{}
    err = nil
    switch val.(type) {
    case int:
      val = int64(val.(int))
      buf := new(bytes.Buffer)
      err = binary.Write(buf, binary.LittleEndian, val)
      temp = buf.Bytes()
    case string:
      temp = []byte(val.(string))
    default:
      buf := new(bytes.Buffer)
      err = binary.Write(buf, binary.LittleEndian, val)
      temp = buf.Bytes()
    }
    if len(temp) > 255 || err != nil {
      log.Fatal("Mashing failed", len(temp), err)
    }
    ans = append(ans, byte(len(temp)))
    ans = append(ans, temp...)
  }
  return ans
}

func main() {
  fmt.Println(getBytes("Hello, playground"))
}



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
