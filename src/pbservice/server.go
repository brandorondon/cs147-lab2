package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  current_view viewservice.View
  kv map[string]string
  // Your declarations here.
}
//RPC to transfer KV map to backup
func(pb *PBServer) Transfer(args *TransArgs, reply *TransReply) error{
  pb.kv = args.KV_map
  reply.Err = TransComplete
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  //check if i am primary...
  if pb.current_view.Primary == pb.me {
    //...if i am then check if value is there
    val, there := pb.kv[args.Key]
    if there {
      reply.Err = OK
      reply.Value = val
    } else {
      reply.Err = ErrNoKey
    }
  } else {
    reply.Err = ErrWrongServer
  }
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  pb.kv[args.Key] = args.Value
  //if primary then forward to backup
  if pb.current_view.Primary == pb.me{
    args.Forwared_req = true
    call(pb.current_view.Backup, "PBServer.Put", args, reply)
    reply.Err = OK
  } else {
    reply.Err = ErrWrongServer
  }
  return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
//READ ME: My program fails occasionally on the Repeated failures/restarts; unreliable
//test; originally it would deadlock but not it occasionally fails after having
//moved the lock/unlock around...any idea why? Should tick() even be locked?
func (pb *PBServer) tick() {
  //ping VS and get new view
  pb.vs.Ping(pb.current_view.Viewnum)
  view, _:= pb.vs.Get()
  //if view changed then set up transfer arguments
  pb.mu.Lock()
  defer pb.mu.Unlock()
  if view != pb.current_view{
    pb.current_view = view
    args := &TransArgs{pb.kv}
    reply := &TransReply{}
    //transfer is primary and backup is there
    if pb.me == pb.current_view.Primary && pb.current_view.Backup != ""{
      call(view.Backup,"PBServer.Transfer",args, reply)
    }
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.current_view = viewservice.View{0,"",""}
  pb.kv = map[string]string{}
  // Your pb.* initializations here.

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
