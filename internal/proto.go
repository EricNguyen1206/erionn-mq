package internal

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	TypePublish   byte = 0x01
	TypeSubscribe byte = 0x02
	TypeMessage   byte = 0x03
	TypeAck       byte = 0x04
	maxBody            = 1 << 20
)

type Frame struct {
	Type byte
	Body []byte
}

func (f *Frame) Encode(w io.Writer) error {
	if len(f.Body) > maxBody {
		return fmt.Errorf("body too large")
	}
	var h [5]byte
	h[0] = f.Type
	binary.BigEndian.PutUint32(h[1:], uint32(len(f.Body)))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(f.Body)
	return err
}

func Decode(r io.Reader) (*Frame, error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n > maxBody {
		return nil, fmt.Errorf("frame too large")
	}
	body := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
	}
	return &Frame{Type: h[0], Body: body}, nil
}