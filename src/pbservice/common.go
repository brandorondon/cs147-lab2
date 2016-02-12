package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
  TransComplete = "TransferCompleted"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  Forwared_req bool
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
}

type GetReply struct {
  Err Err
  Value string
}

type TransArgs struct{
  KV_map map[string]string
}

type TransReply struct{
  Err Err
}


// type ViewCheckArgs struct {
//
// }
//
// type ViewCheckReply struct{
//   View View
// }
