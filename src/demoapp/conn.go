package main

import (
  "github.com/gorilla/websocket"
  "net/http"
  "strings"
  "fmt"
  "github.com/kennygrant/sanitize"
)

type connection struct {
  // The websocket connection.
  ws *websocket.Conn

  // Buffered channel of outbound messages.
  send chan []byte
}

func (c *connection) reader() {
  for {
    _, message, err := c.ws.ReadMessage()
    if err != nil {
      break
    }
    hello := strings.Split(sanitize.HTML(string(message)), "|")
    ck.Put(hello[0], hello[1])
  }
  c.ws.Close()
}

func (c *connection) writer() {
  for message := range c.send {
    err := c.ws.WriteMessage(websocket.TextMessage, message)
    if err != nil {
      break
    }
  }
  c.ws.Close()
}

var upgrader = &websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 1024}

func wsHandler(w http.ResponseWriter, r *http.Request) {
  ws, err := upgrader.Upgrade(w, r, nil)
  if err != nil {
    fmt.Printf("Error, %s\n", err)
    return
  }
  c := &connection{send: make(chan []byte, 256), ws: ws}
  h.register <- c
  defer func() { h.unregister <- c }()
  go c.writer()
  c.reader()
}
