package rpc

type RpcFunc func(req, resp interface{}) error

type Server struct {
	obj interface{}
}

type registration struct {
	f         RpcFunc
	req, resp interface{}
}

func NewServer(obj interface{}) *Server {
	return &Server{
		obj: obj,
	}
}

// func (s *Server) ServeConn(conn net.Conn) error {
// 	for {
// 		//dprintf("server: ready to read a request")
// 		var length uint16
// 		if err := binary.Read(conn, binary.BigEndian, &length); err == io.EOF {
// 			return nil
// 		} else if err != nil {
// 			return fmt.Errorf("read length: %v", err)
// 		}

// 		reqBuf := make([]byte, length)
// 		if _, err := io.ReadFull(conn, reqBuf); err != nil {
// 			return fmt.Errorf("read message: err")
// 		}
// 		id := reqBuf[0]
// 		tag := reqBuf[1]
// 		reqBuf = reqBuf[2:]

// 		//dprintf("server: read a request tag=%d size=%d", tag, len(reqBuf))

// 		s.mu.RLock()
// 		reg, ok := s.funcs[id]
// 		s.mu.RUnlock()
// 		if !ok {
// 			return fmt.Errorf("rpc: unregistered function: id=%d", id)
// 		}

// 		if err := decode(reqBuf, &reg.req); err != nil {
// 			return err
// 		}

// 		if err := reg.f(reg.req, &reg.resp); err != nil {
// 			return err
// 		}

// 		//log.Printf("server: encoding response type=%T tag=%d", resp, tag)
// 		respBuf := encode(reg.resp, id+1, tag)

// 		var buf bytes.Buffer
// 		binary.Write(&buf, binary.BigEndian, uint16(len(respBuf)))
// 		buf.Write(respBuf)
// 		if _, err := buf.WriteTo(conn); err != nil {
// 			return err
// 		}
// 		//dprintf("server: flushed response")
// 	}
// }
