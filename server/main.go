package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/EricNguyen1206/erionn-mq/internal"
)

const (
	addr     = ":9000"
	poolSize = 10
	bufSize  = 256
)

type broker struct {
	mu   sync.RWMutex
	subs map[string]chan *internal.Frame
	pool chan struct{}
}

func newBroker() *broker {
	p := make(chan struct{}, poolSize)
	for i := 0; i < poolSize; i++ {
		p <- struct{}{}
	}
	return &broker{subs: make(map[string]chan *internal.Frame), pool: p}
}

func (b *broker) acquire(id string) bool {
	select {
	case <-b.pool:
		log.Printf("[pool] +%s (%d left)", id, len(b.pool))
		return true
	default:
		log.Printf("[pool] FULL, reject %s", id)
		return false
	}
}

func (b *broker) release(id string) {
	b.pool <- struct{}{}
	log.Printf("[pool] -%s (%d left)", id, len(b.pool))
}

func (b *broker) broadcast(f *internal.Frame) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id, ch := range b.subs {
		select {
		case ch <- f:
		default:
			log.Printf("[broker] drop → %s", id)
		}
	}
}

func (b *broker) handle(conn net.Conn) {
	id := conn.RemoteAddr().String()
	if !b.acquire(id) {
		conn.Close()
		return
	}
	defer b.release(id)
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	r, w := bufio.NewReader(conn), bufio.NewWriter(conn)

	first, err := internal.Decode(r)
	if err != nil {
		return
	}
	conn.SetReadDeadline(time.Time{})

	ack := &internal.Frame{Type: internal.TypeAck, Body: []byte("OK")}
	ack.Encode(w)
	w.Flush()

	switch first.Type {
	case internal.TypePublish:
		log.Printf("[server] publisher: %s", id)
		b.broadcast(&internal.Frame{Type: internal.TypeMessage, Body: stamp(first.Body)})
		for {
			f, err := internal.Decode(r)
			if err != nil {
				if err != io.EOF {
					log.Printf("[pub] %s: %v", id, err)
				}
				return
			}
			b.broadcast(&internal.Frame{Type: internal.TypeMessage, Body: stamp(f.Body)})
		}

	case internal.TypeSubscribe:
		log.Printf("[server] subscriber: %s", id)
		ch := make(chan *internal.Frame, bufSize)
		b.mu.Lock()
		b.subs[id] = ch
		b.mu.Unlock()
		defer func() {
			b.mu.Lock()
			delete(b.subs, id)
			b.mu.Unlock()
		}()
		for f := range ch {
			if err := f.Encode(w); err != nil {
				return
			}
			w.Flush()
		}
	}
}

func stamp(body []byte) []byte {
	return []byte(fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05.000"), body))
}

func main() {
	b := newBroker()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[server] listening on %s  pool=%d", addr, poolSize)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go b.handle(conn)
	}
}