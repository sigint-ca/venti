package venti

const (
	rpcPing    = 2
	rpcHello   = 4
	rpcGoodbye = 6
	rpcAuth0   = 8
	rpcAuth1   = 10
	rpcRead    = 12
	rpcWrite   = 14
	rpcSync    = 16
)

type helloRequest struct {
	Version  string
	Uid      string
	Strength uint8
	Crypto   string "short"
	Codec    string "short"
}

type helloResponse struct {
	Sid     string
	Rcrypto uint8
	Rcodec  uint8
}

type readRequest struct {
	Score Score
	Type  uint8
	Pad   uint8
	Count uint16
}

type readResponse struct {
	Data []byte
}

type writeRequest struct {
	Type uint8
	Pad  [3]uint8
	Data []byte
}

type writeResponse struct {
	Score Score
}
