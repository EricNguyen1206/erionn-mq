package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/EricNguyen1206/erionn-mq/internal"
)

func main() {
	conn, _ := net.Dial("tcp", ":1234")
	defer conn.Close()

	r, w := bufio.NewReader(conn), bufio.NewWriter(conn)

	(&internal.Frame{Type: internal.TypePublish, Body: []byte("hi")}).Encode(w)
	w.Flush()

	ack, _ := internal.Decode(r)
	if string(ack.Body) != "OK" {
		log.Fatalf("handshake failed: %v", ack.Body)
	}
	fmt.Println("Provider ready. Ctrl+D to quit.")

	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		(&internal.Frame{Type: internal.TypePublish, Body: []byte(text)}).Encode(w)
		w.Flush()
		fmt.Printf("→ %q\n", text)
	}
}