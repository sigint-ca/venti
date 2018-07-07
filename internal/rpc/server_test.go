package rpc

import (
	"fmt"
	"testing"
)

func TestRPCServer(t *testing.T) {
	type In struct {
		Foo int
	}
	type Out struct {
		Bar int
	}
	funcs := map[int]interface{}{
		1: func(req In, res *Out) error {
			fmt.Println("test")
			return nil
		},
	}
	s := NewServer(funcs)

	req, res := s.getRpcFuncInputs(1)
	fmt.Printf("req=%T res=%T\n", req, res)

	f := s.getRpcFunc(1)
	fmt.Printf("f=%T\n", f)
}
