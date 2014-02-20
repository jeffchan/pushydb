package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
  ErrDupRequest = "ErrDupRequest"
  ErrOutOfSync = "ErrOutOfSync"
  ErrBusy = "ErrBusy"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  Id string
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type PutRelayArgs struct {
  Key string
  Value string
  PreviousValue string
  Id string
}

type PutRelayReply struct {
  Err Err
}

type GetArgs struct {
  Key string
  Id string
}

type GetReply struct {
  Err Err
  Value string
}

type GetRelayArgs struct {
  Key string
  Value string
  Id string
}

type GetRelayReply struct {
  Err Err
}

type SyncArgs struct {
  To string
}

type SyncReply struct {
  Table map[string]string
  Reqs map[string]string
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

