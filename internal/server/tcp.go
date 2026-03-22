package server

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/EricNguyen1206/erionn-mq/internal/broker"
)

type TCPServer struct {
	addr   string
	broker *broker.Broker
}

func NewTCPServer(addr string, b *broker.Broker) *TCPServer {
	return &TCPServer{
		addr:   addr,
		broker: b,
	}
}

func (s *TCPServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	log.Printf("broker listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go s.handleConn(conn)
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()

	log.Printf("client connected: %s", conn.RemoteAddr())

	handler := NewConnectionHandler(s.broker, conn)

	if err := handler.Serve(); err != nil && err != io.EOF {
		log.Printf("connection error from %s: %v", conn.RemoteAddr(), err)
		_, _ = fmt.Fprintf(conn, "ERR %v\n", err)
	}

	log.Printf("client disconnected: %s", conn.RemoteAddr())
}
