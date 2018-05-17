package venti

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
)

const VentiPort = 17034

var supportedVersions = []string{
	"02",
}

type Server struct {
	backend Backend
}

type conn struct {
	server *Server

	// the underlying network connection
	rwc net.Conn

	bufr *bufio.Reader
	bufw *bufio.Writer

	version string
	uid     string
}

func NewServer(b Backend) (*Server, error) {
	return &Server{backend: b}, nil
}

func (s *Server) Listen(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for {
		rwc, err := l.Accept()
		if err != nil {
			log.Print(err)
			continue
		}

		go func() {
			c := &conn{
				server: s,
				rwc:    rwc,
				bufr:   bufio.NewReader(rwc),
				bufw:   bufio.NewWriter(rwc),
			}
			defer c.close()

			if err := c.serve(); err != nil {
				log.Printf("serve: %v", err)
			}
		}()
	}
}

func (c *conn) close() error {
	dprintf("server: closing connection")
	return c.rwc.Close()
}

func (c *conn) serve() error {
	return errors.New("not implemented")
	// if err := c.negotiateVersion(); err != nil {
	// 	return err
	// }

	// for {
	// 	dprintf("server: ready to read a request")
	// 	var length uint16
	// 	if err := binary.Read(c.bufr, binary.BigEndian, &length); err == io.EOF {
	// 		return nil
	// 	} else if err != nil {
	// 		return fmt.Errorf("read length: %v", err)
	// 	}

	// 	reqBuf := make([]byte, length)
	// 	if _, err := io.ReadFull(c.bufr, reqBuf); err != nil {
	// 		return fmt.Errorf("read message: err")
	// 	}
	// 	typ := reqBuf[0]
	// 	tag := reqBuf[1]
	// 	reqBuf = reqBuf[2:]

	// 	dprintf("server: read a request tag=%d size=%d", tag, len(reqBuf))

	// 	var resp message
	// 	var err error
	// 	switch typ {
	// 	case rpcPing:
	// 		req := new(pingRequest)
	// 		if err := req.decode(reqBuf); err != nil {
	// 			err = fmt.Errorf("server failed to decode request: %v", err)
	// 			break
	// 		}

	// 		resp = &pingResponse{}

	// 	case rpcHello:
	// 		req := new(helloRequest)
	// 		if err = req.decode(reqBuf); err != nil {
	// 			err = fmt.Errorf("server failed to decode request: %v", err)
	// 			break
	// 		}

	// 		c.version = req.version
	// 		c.uid = req.uid

	// 		resp = &helloResponse{
	// 			sid: "foobar",
	// 		}

	// 	// case rpcGoodbye:
	// 	// case rpcAuth0:
	// 	// case rpcAuth1:
	// 	// case rpcRead:
	// 	case rpcWrite:
	// 		req := new(writeRequest)
	// 		if err := req.decode(reqBuf); err != nil {
	// 			err = fmt.Errorf("server failed to decode request: %v", err)
	// 			break
	// 		}

	// 		log.Printf("server: writing block: %q", req.data)
	// 		s, err := c.server.backend.WriteBlock(req.typ, req.data)
	// 		if err != nil {
	// 			err = fmt.Errorf("server failed to write block: %v", err)
	// 			break
	// 		}
	// 		log.Printf("server: wrote block with score %v", s)

	// 		resp = &writeResponse{
	// 			score: s,
	// 		}

	// 	// case rpcSync:
	// 	default:
	// 		err = fmt.Errorf("request type not recognized: %d", typ)
	// 	}

	// 	if err != nil {
	// 		resp = &errorResponse{err: err}
	// 	}

	// 	log.Printf("server: encoding response type=%T tag=%d", resp, tag)
	// 	respBuf := resp.encode(tag)

	// 	var buf bytes.Buffer
	// 	binary.Write(&buf, binary.BigEndian, uint16(len(respBuf)))
	// 	buf.Write(respBuf)
	// 	buf.WriteTo(c.bufw)
	// 	if err := c.bufw.Flush(); err != nil {
	// 		return fmt.Errorf("flush response: %v", err)
	// 	}
	// 	dprintf("server: flushed response")
	// }
}

func (c *conn) negotiateVersion() error {
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

	clientSupported := strings.Split(parts[1], ":")
	if len(clientSupported) == 0 {
		return fmt.Errorf("bad version string: %q", vs)
	}

	serverSupported := strings.Join(supportedVersions, ":")
	vs = fmt.Sprintf("venti-%s-sigint.ca/venti\n", serverSupported)
	if _, err := c.bufw.WriteString(vs); err != nil {
		return err
	}

	return c.bufw.Flush()
}
