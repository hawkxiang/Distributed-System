package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client int64
	Seq    int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client int64
	Seq    int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type ScanArgs struct {
	Low string
	High string

	Client int64
	Seq    int
}

type ScanReply struct {
	WrongLeader bool
	Err         Err
	Content      map[string]string
}
