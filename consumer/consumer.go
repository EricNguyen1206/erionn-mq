//go:build ignore

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/EricNguyen1206/erionn-mq/internal"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	r, w := bufio.NewReader(conn), bufio.NewWriter(conn)

	(&internal.Frame{Type: internal.TypeSubscribe}).Encode(w)
	w.Flush()

	ack, err := internal.Decode(r)
	if err != nil || string(ack.Body) != "OK" {
		log.Fatalf("handshake failed: %v", err)
	}
	fmt.Println("Subscribed. Waiting for messages...")

	for {
		f, err := internal.Decode(r)
		if err == io.EOF {
			fmt.Println("Server closed.")
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("MSG: %s", f.Body)
	}
}