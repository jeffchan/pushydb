package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
}

func MakeWorkerInfo(address string) *WorkerInfo {
  w := new(WorkerInfo)
  w.address = address
  return w
}

func MakeDoJobArgs(file string, op JobType, job int, numOtherPhase int) *DoJobArgs{
  args := &DoJobArgs{}
  args.File = file
  args.Operation = op
  args.JobNumber = job
  args.NumOtherPhase = numOtherPhase
  return args
}

func (mr *MapReduce) DispatchJobs() {
  for {
    address := <- mr.idleChannel
    w := mr.Workers[address]
    args := <- mr.jobs

    go mr.SendJob(w, args)
  }
}

func (mr *MapReduce) SendJob(w *WorkerInfo, args *DoJobArgs) {
  var reply DoJobReply
  ok := call(w.address, "Worker.DoJob", args, &reply)
  if ok {
    // job completed
    mr.doneJobs <- args
    mr.idleChannel <- w.address
  } else {
    // job failed
    mr.jobs <- args
    fmt.Printf("RPC %s DoJob error\n", w.address)
  }
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

func (mr *MapReduce) RegisterWorkers() {
  for {
    worker := <- mr.registerChannel
    mr.Workers[worker] = MakeWorkerInfo(worker)
    mr.idleChannel <- worker
    fmt.Println("Registerd worker: " + worker)
  }
}

func (mr *MapReduce) RunMaster() *list.List {
  DPrintf("RunMaster %s\n", mr.MasterAddress)

  go mr.RegisterWorkers()

  // Assign map jobs
  for job := 0; job < mr.nMap; job++ {
    mr.jobs <- MakeDoJobArgs(mr.file, Map, job, mr.nReduce)
  }

  go mr.DispatchJobs()

  // Wait for map jobs to complete
  for job := 0; job < mr.nMap; job++ {
    <- mr.doneJobs
  }

  // Assign reduce jobs
  for job := 0; job < mr.nReduce; job++ {
    mr.jobs <- MakeDoJobArgs(mr.file, Reduce, job, mr.nMap)
  }

  go mr.DispatchJobs()

  // Wait for reduce jobs to complete
  for job := 0; job < mr.nReduce; job++ {
    <- mr.doneJobs
  }

  return mr.KillWorkers()
}
