package main

import "github.com/go-martini/martini"
// import "messagebroker"

// game plan: use pushydb as actual db
// set up infrastructure on main() call, spawn goroutine to listen on channel
// interface: form: subscribe to a key
// form: put to a key
// view: see realtime push from server
// how to get server > client ?
// easiest way- just do long-polling. Could use Gorilla socket...may be hard

func main() {
  m := martini.Classic()
  m.Get("/", func() string {
    return "Hello world!"
  })
  m.Run()
}
