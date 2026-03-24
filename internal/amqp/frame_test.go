package amqp

import (
	"bytes"
	"reflect"
	"testing"
)

func TestProtocolHeader_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteProtocolHeader(&buf); err != nil {
		t.Fatalf("WriteProtocolHeader: %v", err)
	}
	if err := ReadProtocolHeader(&buf); err != nil {
		t.Fatalf("ReadProtocolHeader: %v", err)
	}
}

func TestFrame_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	frame := Frame{Type: FrameMethod, Channel: 7, Payload: []byte("hello")}
	if err := WriteFrame(&buf, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	got, err := ReadFrame(&buf, defaultFrameMax)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Type != frame.Type || got.Channel != frame.Channel || !bytes.Equal(got.Payload, frame.Payload) {
		t.Fatalf("round trip mismatch: got=%+v want=%+v", got, frame)
	}
}

func TestContentHeader_RoundTrip(t *testing.T) {
	head := ContentHeader{
		ClassID:  classBasic,
		BodySize: 5,
		Properties: BasicProperties{
			ContentType:   "text/plain",
			CorrelationID: "cid-1",
			ReplyTo:       "reply-queue",
			DeliveryMode:  2,
			Headers: Table{
				"bool":   true,
				"nested": Table{"key": "value"},
				"number": int32(42),
			},
		},
	}

	frame, err := EncodeContentHeaderFrame(1, head)
	if err != nil {
		t.Fatalf("EncodeContentHeaderFrame: %v", err)
	}

	got, err := DecodeContentHeaderFrame(frame)
	if err != nil {
		t.Fatalf("DecodeContentHeaderFrame: %v", err)
	}

	if got.ClassID != head.ClassID || got.BodySize != head.BodySize {
		t.Fatalf("header mismatch: got=%+v want=%+v", got, head)
	}
	if !reflect.DeepEqual(got.Properties, head.Properties) {
		t.Fatalf("properties mismatch: got=%#v want=%#v", got.Properties, head.Properties)
	}
}

func TestReadFrame_RejectsPayloadOverFrameMax(t *testing.T) {
	var buf bytes.Buffer
	frame := Frame{Type: FrameMethod, Channel: 1, Payload: []byte("12345")}
	if err := WriteFrame(&buf, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	_, err := ReadFrame(&buf, 4)
	if err == nil {
		t.Fatal("expected frame-max validation error, got nil")
	}
}

func TestReadFrame_RejectsPayloadOverRemainingBytes(t *testing.T) {
	data := []byte{
		FrameMethod,
		0, 1,
		0, 0, 0, 5,
		'a', 'b',
	}

	_, err := ReadFrame(bytes.NewReader(data), defaultFrameMax)
	if err == nil {
		t.Fatal("expected remaining-bytes validation error, got nil")
	}
}

func TestReadFrame_RejectsMissingFrameTerminatorBeforeAllocation(t *testing.T) {
	data := []byte{
		FrameMethod,
		0, 1,
		0, 0, 0, 2,
		'a', 'b',
	}

	_, err := ReadFrame(bytes.NewReader(data), defaultFrameMax)
	if err == nil {
		t.Fatal("expected missing-terminator validation error, got nil")
	}
}

func TestReadLongstr_RejectsLengthOverRemainingBytes(t *testing.T) {
	r := bytes.NewReader([]byte{0, 0, 0, 5, 'a', 'b'})
	_, err := readLongstr(r)
	if err == nil {
		t.Fatal("expected longstr validation error, got nil")
	}
}

func TestReadTable_RejectsLengthOverRemainingBytes(t *testing.T) {
	r := bytes.NewReader([]byte{0, 0, 0, 4, 'a'})
	_, err := readTable(r)
	if err == nil {
		t.Fatal("expected table validation error, got nil")
	}
}

func TestReadArray_RejectsLengthOverRemainingBytes(t *testing.T) {
	r := bytes.NewReader([]byte{0, 0, 0, 4, 'b'})
	_, err := readArray(r)
	if err == nil {
		t.Fatal("expected array validation error, got nil")
	}
}
