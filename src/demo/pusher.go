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
      if pub.Type == messagebroker.Put {
        postData(pub.PutArgs)
      }
    }
  }()

  return demo
}

func postData(args messagebroker.NotifyPutArgs) {
  values := make(url.Values)
  values.Set("key", args.Key)
  values.Set("value", args.Value)
  values.Set("reqid", args.ReqId)
  r, err := http.PostForm("http://127.0.0.1:3000/post", values)
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
