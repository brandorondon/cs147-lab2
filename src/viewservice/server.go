package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

//Brando Rondon
//Lab2 Part A
//Spring 2016

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string
  server_responses map[string]time.Time
  current_view View
  fresh_b bool
  fresh_p bool
  server_status map[string]string
  last_sent_view map[string]uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  vs.server_responses[args.Me] = time.Now()
  vs.last_sent_view[args.Me] = args.Viewnum

  if vs.current_view.Primary == "" && vs.fresh_p{ //first primary to connect
    vs.current_view.Primary = args.Me
    reply.View.Primary = vs.current_view.Primary
    vs.current_view.Viewnum++
    vs.fresh_p = false
  //first backup to connect
  } else if vs.fresh_b && args.Me != vs.current_view.Primary && vs.current_view.Backup == ""{
    vs.current_view.Backup = args.Me
    reply.View.Backup = vs.current_view.Backup
    vs.current_view.Viewnum++
    vs.fresh_b = false
  //serve crash and restart detection; prevents from it keeping connected
  } else if args.Viewnum == 0 && args.Me == vs.current_view.Primary{
    vs.current_view.Primary = vs.current_view.Backup
    vs.current_view.Backup = ""
    reply.View = vs.current_view
    vs.current_view.Viewnum++
  } else {
    reply.View = vs.current_view
  }
  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  reply.View = vs.current_view
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick(){
  vs.mu.Lock()
  defer vs.mu.Unlock()
  change := false
  prim_last_view := vs.last_sent_view[vs.current_view.Primary]
  //check if the primary's last sent view matches our current view, else we
  //don't do any sever promotion
  if prim_last_view == vs.current_view.Viewnum{
    //iterate over the list of pinged server
    for k, v := range vs.server_responses{
      since := time.Since(v)
      //if our primary is down, promote back
      if since > DeadPings*PingInterval && k == vs.current_view.Primary {
        vs.current_view.Primary = vs.current_view.Backup
        vs.current_view.Backup = ""
        change = true
      //promote an idle server as backup
      } else if vs.current_view.Backup == "" && vs.current_view.Primary != k && since < DeadPings*PingInterval{
        vs.current_view.Backup = k
        change = true
      //disconnect the backup if it's dead
      } else if since > DeadPings*PingInterval && k == vs.current_view.Backup{
        vs.current_view.Backup = ""
        change = true
      }
    }
    //increment the view if any of the conditonals above were executed, which means
    //some sort of server promotion took place
    if change {
      vs.current_view.Viewnum++
    }
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  vs.server_responses = map[string]time.Time{}
  vs.current_view = View{0,"",""}
  vs.fresh_p = true
  vs.fresh_b = true
  vs.last_sent_view = map[string]uint{}
  //vs.server_status = map[string]string{}
  //primary_last_view = 0
  // Your vs.* initializations here.

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
