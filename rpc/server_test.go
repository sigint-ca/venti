package rpc_test

import (
	"testing"
)

func TestServeConn(t *testing.T) {
	// cliConn, srvConn := net.Pipe()

	// srv := rpc.NewServer()
	// srv.Register(1, TestRPC{})
	// go func() {
	// 	if err := srv.ServeConn(srvConn); err != nil {
	// 		t.Fatal(err)
	// 	}
	// }()

	// cli := rpc.NewClient(cliConn)

	// req := TestReq{Foo: 1}
	// var resp TestResp
	// cli.Call(1, req, &resp)
}

type TestRPC struct {
}

func (t *TestRPC) Inc(arg int, ret *int) error {
	*ret = arg + 1
	return nil
}
