package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  mapDone chan bool
  reduceDone chan bool
  mapJobs *list.List
  reduceJobs *list.List
}

func MakeWorkerInfo(address string) *WorkerInfo {
  w := new(WorkerInfo)
  w.address = address
  w.mapDone = make(chan bool)
  w.reduceDone = make(chan bool)
  w.mapJobs = list.New()
  w.reduceJobs = list.New()
  return w
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  DPrintf("RunMaster %s\n", mr.MasterAddress)

  var worker1, worker2 string
  worker1 = <- mr.registerChannel
  worker2 = <- mr.registerChannel

  mr.Workers[worker1] = MakeWorkerInfo(worker1)
  mr.Workers[worker2] = MakeWorkerInfo(worker2)

  fmt.Println("sup", len(mr.Workers));

  index := 0
  for _, w := range mr.Workers {
    for job := index; job < mr.nMap; job += len(mr.Workers) {
      w.mapJobs.PushBack(job)
    }
    for job := index; job < mr.nReduce; job += len(mr.Workers)  {
      w.reduceJobs.PushBack(job)
    }
    index++
  }

  for _, w := range mr.Workers {
    go func(w *WorkerInfo) {
      for e := w.mapJobs.Front(); e != nil; e = e.Next() {
        args := &DoJobArgs{}
        args.File = mr.file
        args.Operation = Map
        args.JobNumber = e.Value.(int)
        args.NumOtherPhase = mr.nReduce
        var reply DoJobReply
        ok := call(w.address, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("RPC %s DoJob error\n", w.address)
        }
      }

      w.mapDone <- true
    }(w)
  }

  // Block until Map phase complete
  for _, w := range mr.Workers {
    <- w.mapDone
  }

  for _, w := range mr.Workers {
    go func(w *WorkerInfo) {
      for e := w.reduceJobs.Front(); e != nil; e = e.Next() {
        args := &DoJobArgs{}
        args.File = mr.file
        args.Operation = Reduce
        args.JobNumber = e.Value.(int)
        args.NumOtherPhase = mr.nMap
        var reply DoJobReply
        ok := call(w.address, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("RPC %s DoJob error\n", w.address)
        }
      }

      w.reduceDone <- true
    }(w)
  }

  // Block until Reduce phase complete
  for _, w := range mr.Workers {
    <- w.reduceDone
  }
  return mr.KillWorkers()
}
