package venti

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"sigint.ca/venti2/rpc"
)

type Client struct {
	// the underlying network connection
	rwc net.Conn

	// buffered reader of rwc
	bufr *bufio.Reader

	version string
	uid     string
	sid     string

	rpc *rpc.Client
}

func Dial(ctx context.Context, address string) (*Client, error) {
	var d net.Dialer
	rwc, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}

	c := &Client{
		rwc:  rwc,
		bufr: bufio.NewReader(rwc),
		uid:  "foobar",
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := c.rwc.SetDeadline(deadline); err != nil {
			return nil, err
		}
	} else {
		c.rwc.SetDeadline(time.Time{})
	}

	if err := c.negotiateVersion(); err != nil {
		return nil, fmt.Errorf("handshake: %v", err)
	}
	dprintf("client: choosing version %s", c.version)

	c.rpc = rpc.NewClient(rwc)

	if err := c.hello(ctx); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Client) negotiateVersion() error {
	vs := fmt.Sprintf("venti-%s-sigint.ca/venti\n", strings.Join(supportedVersions, ":"))
	if _, err := c.rwc.Write([]byte(vs)); err != nil {
		return err
	}

	vs, err := c.bufr.ReadString('\n')
	if err != nil {
		return err
	}

	parts := strings.Split(vs, "-")
	if len(parts) != 3 {
		return fmt.Errorf("bad version string: %q", vs)
	}
	if parts[0] != "venti" {
		return fmt.Errorf("bad version string: %q", vs)
	}

	serverSupported := strings.Split(parts[1], ":")
	if len(serverSupported) == 0 {
		return fmt.Errorf("bad version string: %q", vs)
	}

	for _, v := range serverSupported {
		for _, vv := range supportedVersions {
			if v == vv {
				c.version = v
				return nil
			}
		}
	}

	return errors.New("failed to negotiate version")
}

func (c *Client) hello(ctx context.Context) error {
	req := helloRequest{
		Version: c.version,
		Uid:     c.uid,
	}
	var res helloResponse
	if err := c.rpc.Call(ctx, rpcHello, req, &res); err != nil {
		return fmt.Errorf("hello: %v", err)
	}
	c.sid = res.Sid

	return nil
}

func (c *Client) Ping(ctx context.Context) error {
	var req, res struct{}
	if err := c.rpc.Call(ctx, rpcPing, req, &res); err != nil {
		if _, ok := err.(rpc.ServerError); ok {
			// The plan9 venti server responds to pings with
			// an error. Treat this as a ping response.
			return nil
		}
		return fmt.Errorf("ping: %v", err)
	}

	return nil
}

func (c *Client) ReadBlock(ctx context.Context, dst []byte, blockType uint8, s Score) error {
	req := readRequest{
		Score: s,
		Type:  blockType,
		Count: uint16(len(dst)),
	}
	res := readResponse{
		Data: dst,
	}
	if err := c.rpc.Call(ctx, rpcRead, req, &res); err != nil {
		return fmt.Errorf("read: %v", err)
	}

	return nil
}

func (c *Client) WriteBlock(ctx context.Context, blockType uint8, data []byte) (Score, error) {
	req := writeRequest{
		Data: data,
		Type: blockType,
	}
	var res writeResponse
	if err := c.rpc.Call(ctx, rpcWrite, req, &res); err != nil {
		return Score{}, fmt.Errorf("write: %v", err)
	}

	return res.Score, nil
}

func (c *Client) Sync(ctx context.Context) error {
	var req, res struct{}
	if err := c.rpc.Call(ctx, rpcSync, req, &res); err != nil {
		return fmt.Errorf("sync: %v", err)
	}
	return nil
}

func (c *Client) Close() error {
	dprintf("client: closing connection")
	return c.rwc.Close()
}
