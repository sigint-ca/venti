package venti

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"sigint.ca/venti2/rpc"
)

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

const VentiPort = 17034

var supportedVersions = []string{
	"02",
}

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

func (c *Client) goodbye(ctx context.Context) error {
	panic("TODO")
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

type readRequest struct {
	Score Score
	Type  uint8
	Pad   uint8
	Count uint16
}

type readResponse struct {
	Data []byte
}

func (c *Client) ReadBlock(s Score, t BlockType, buf []byte) (int, error) {
	return c.ReadBlockContext(context.TODO(), s, t, buf)
}

func (c *Client) ReadBlockContext(ctx context.Context, s Score, t BlockType, buf []byte) (int, error) {
	if len(buf) > math.MaxUint16 {
		return 0, errors.New("oversized buffer")
	}

	req := readRequest{
		Score: s,
		Type:  uint8(t),
		Count: uint16(len(buf)),
	}
	res := readResponse{
		Data: buf,
	}
	if err := c.rpc.Call(ctx, rpcRead, req, &res); err != nil {
		return 0, fmt.Errorf("read: %v", err)
	}

	return len(res.Data), nil
}

type writeRequest struct {
	Type uint8
	Pad  [3]uint8
	Data []byte
}

type writeResponse struct {
	Score Score
}

func (c *Client) WriteBlock(t BlockType, buf []byte) (Score, error) {
	return c.WriteBlockContext(context.TODO(), t, buf)
}

func (c *Client) WriteBlockContext(ctx context.Context, t BlockType, buf []byte) (Score, error) {
	if len(buf) > math.MaxUint16 {
		return Score{}, errors.New("oversized buffer")
	}

	req := writeRequest{
		Data: buf,
		Type: uint8(t),
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
	return c.rwc.Close()
}