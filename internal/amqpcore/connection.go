package amqpcore

import "net"

// Connection represents a client TCP connection to the broker.
// At this stage it is a lightweight metadata holder; the AMQP protocol
// layer (Phase 2) will attach the full state machine here.
type Connection struct {
	ID    uint64
	Conn  net.Conn
	VHost string
}
