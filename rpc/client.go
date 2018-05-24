package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn net.Conn

	tags chan uint8

	pendingCond *sync.Cond
	pending     map[uint8]*call

	errmu sync.RWMutex
	err   error
}

type call struct {
	msg  interface{}
	done chan error
	ctx  context.Context
}

func NewClient(conn net.Conn) *Client {
	c := &Client{
		conn: conn,

		pendingCond: sync.NewCond(&sync.Mutex{}),
		pending:     make(map[uint8]*call),
	}

	go c.readResponses()

	return c
}

func (c *Client) Call(ctx context.Context, funcId uint8, req, resp interface{}) error {
	if err := c.getErr(); err != nil {
		return fmt.Errorf("rpc client closed due to error: %v", err)
	}

	tag := c.aqcuireTag()
	defer c.releaseTag(tag)

	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return err
		}
	} else {
		c.conn.SetDeadline(time.Time{})
	}

	encoded, err := encode(req, funcId, tag)
	if err != nil {
		return fmt.Errorf("encode message: %v", err)
	}
	if _, err := c.conn.Write(encoded); err != nil {
		return fmt.Errorf("send message: %v", err)
	}

	done := make(chan error, 1)
	c.pendingCond.L.Lock()
	c.pending[tag] = &call{
		msg:  resp,
		done: done,
		ctx:  ctx,
	}
	c.pendingCond.Signal()
	c.pendingCond.L.Unlock()

	select {
	case err = <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}

	c.pendingCond.L.Lock()
	delete(c.pending, tag)
	c.pendingCond.L.Unlock()

	return err
}

const maxResponse = 64 * 1024

var responseBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, maxResponse)
	},
}

func (c *Client) readResponses() {
	for {
		var length uint16
		if err := binary.Read(c.conn, binary.BigEndian, &length); err != nil {
			c.setErr(err)
			break
		}
		buf := responseBufPool.Get().([]byte)[0:length]
		if _, err := io.ReadFull(c.conn, buf); err != nil {
			c.setErr(err)
			break
		}

		go func(buf []byte) {
			defer responseBufPool.Put(buf)

			// Find the rpc call waiting for this message
			tag := buf[1]
			var ok bool
			var call *call
			c.pendingCond.L.Lock()
			for !ok {
				if call, ok = c.pending[tag]; !ok {
					c.pendingCond.Wait()
					continue
				}
			}
			c.pendingCond.L.Unlock()

			select {
			case <-call.ctx.Done():
			default:
				var err ServerError
				if buf[0] == rpcError {
					call.done <- decode(&err, buf[2:])
				} else {
					call.done <- decode(call.msg, buf[2:])
				}
			}
		}(buf)
	}
	c.conn.Close()
}

func (c *Client) setErr(err error) {
	c.errmu.Lock()
	defer c.errmu.Unlock()
	c.err = err
}

func (c *Client) getErr() error {
	c.errmu.RLock()
	defer c.errmu.RUnlock()
	return c.err
}
