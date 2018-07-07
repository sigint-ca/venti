package ventirpc

import (
	"encoding/binary"
	"errors"
	"io"
	"net/rpc"
	"strconv"
	"sync"
)

type serverCodec struct {
	dec *decoder
	enc *encoder
	c   io.Closer

	req serverRequest

	mutex   sync.Mutex
	seq     uint64
	pending map[uint64]struct{}
}

func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &serverCodec{
		c:       conn,
		pending: make(map[uint64]struct{}),
	}
}

type header struct {
	Length uint16
	Type   uint8
	Tag    uint8
}

type serverRequest struct {
	h      header
	params interface{}
}

func (r *serverRequest) reset() {
	r.h = header{}
	r.params = nil
}

type serverResponse struct {
	Id     uint8
	Result interface{}
	Error  interface{}
}

func (c *serverCodec) ReadRequestHeader(r *rpc.Request) error {
	if err := binary.Read(c.c, binary.BigEndian, &c.req.h); err != nil {
		return err
	}
	c.req.h.Length -= 2

	r.ServiceMethod = c.methodName(c.req.h.Tag)
	r.Seq = uint64(c.req.h.Tag)

	return nil
}

func (c *serverCodec) methodName(t uint8) string {
	return strconv.Itoa(int(t))
}

func (c *serverCodec) ReadRequestBody(x interface{}) error {
	if x == nil {
		return nil
	}

}

func (c *serverCodec) WriteResponse(r *rpc.Response, x interface{}) error {
	if r.Seq > (1<<8)-1 {
		panic("sequence number overflows uint8")
	}

	c.mutex.Lock()
	_, ok := c.pending[r.Seq]
	if !ok {
		c.mutex.Unlock()
		return errors.New("invalid sequence number in response")
	}
	delete(c.pending, r.Seq)
	c.mutex.Unlock()

	resp := serverResponse{Id: uint8(r.Seq)}
	if r.Error == "" {
		resp.Result = x
	} else {
		resp.Error = r.Error
	}
	return c.enc.encode(resp)
}

func (c *serverCodec) Close() error {
	return c.c.Close()
}
