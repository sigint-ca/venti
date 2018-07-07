package rpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
)

type Server struct {
	funcs map[int]interface{}
}

// TODO: doc
// funcs must be a map of rpc func IDs to func(inputType, *outputType) error.
func NewServer(funcs map[int]interface{}) *Server {
	for k, v := range funcs {
		t := reflect.TypeOf(v)
		if t.Kind() != reflect.Func {
			panic(fmt.Sprintf("not a function: funcs[%d]: %T", k, v))
		}
		if t.NumIn() != 2 ||
			t.In(0).Kind() != reflect.Struct ||
			t.In(1).Kind() != reflect.Ptr ||
			t.In(1).Elem().Kind() != reflect.Struct ||
			t.NumOut() != 1 ||
			t.Out(0).Kind() != reflect.Interface ||
			t.Out(0).Name() != "error" {
			panic(fmt.Sprintf("bad signature: funcs[%d]: %T", k, v))
		}

	}
	return &Server{
		funcs: funcs,
	}
}

func (s *Server) getRpcFunc(id int) func(req, res interface{}) error {
	return s.funcs[id].(func(req, res interface{}) error)
}

func (s *Server) getRpcFuncInputs(id int) (req, res interface{}) {
	f := s.funcs[id]

	req = reflect.Zero(reflect.TypeOf(f).In(0)).Interface()
	res = reflect.Zero(reflect.TypeOf(f).In(1)).Interface()
	return
}

func (s *Server) ServeConn(conn net.Conn) error {
	for {
		dprintf("server: ready to read a request")
		var length uint16
		if err := binary.Read(conn, binary.BigEndian, &length); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("read length: %v", err)
		}

		reqBuf := make([]byte, length)
		if _, err := io.ReadFull(conn, reqBuf); err != nil {
			return fmt.Errorf("read message: err")
		}
		id := int(reqBuf[0])
		tag := reqBuf[1]
		reqBuf = reqBuf[2:]

		dprintf("server: read a request tag=%d size=%d", tag, len(reqBuf))

		req, res := s.getRpcFuncInputs(id)
		if err := decode(&req, reqBuf); err != nil {
			return err
		}

		f := s.getRpcFunc(id)

		if err := f(req, res); err != nil {
			return err
		}

		dprintf("server: encoding response type=%T tag=%d", res, tag)
		respBuf, err := encode(res, uint8(id+1), tag)
		if err != nil {
			return err
		}

		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint16(len(respBuf)))
		buf.Write(respBuf)
		if _, err := buf.WriteTo(conn); err != nil {
			return err
		}
		dprintf("server: flushed response")
	}
}

func dprintf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
