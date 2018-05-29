package rpc

const rpcError = 1

type ServerError struct {
	Err string
}

func (s ServerError) Error() string {
	return s.Err
}
