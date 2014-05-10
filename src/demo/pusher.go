package demo

import "fmt"
import "messagebroker"
import "net/http"

// import "io/ioutil"
import "net/url"

type Demo struct {
  pubchan chan messagebroker.PublishArgs
  clerk   *messagebroker.Clerk
  addr    string
}

func MakeDemo() *Demo {
  demo := new(Demo)
  publications := make(chan messagebroker.PublishArgs)
  addr := port("demo-clerk", 0)
  ck := messagebroker.MakeClerk(addr, publications)

  demo.pubchan = publications
  demo.clerk = ck
  demo.addr = addr

  go func() {
    for {
      pub := <-demo.pubchan
      fmt.Printf("Received notification- posting\n")
      postData(pub)
    }
  }()

  return demo
}

func postData(pub messagebroker.PublishArgs) {
  values := make(url.Values)
  values.Set("key", pub.Key)
  values.Set("value", pub.Value)
  values.Set("reqid", pub.ReqId)
  r, err := http.PostForm("http://127.0.0.1:5000/push", values)
  if err != nil {
    fmt.Printf("error posting: %s", err)
    return
  }
  // body, _ := ioutil.ReadAll(r.Body)
  r.Body.Close()
}

func main() {
  MakeDemo()
}
