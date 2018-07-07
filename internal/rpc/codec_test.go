package rpc

import (
	"bytes"
	"testing"
)

type TestMessage struct {
	Int   uint16
	Str   string
	Small []byte `rpc:"small"`
	Arr   [5]byte
	Buf   []byte
}

func TestCodec(t *testing.T) {
	in := TestMessage{
		Int:   0xabcd,
		Str:   "foobar",
		Small: []byte("test"),
		Arr:   [5]byte{1, 2, 3, 4, 5},
		Buf:   []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}

	buf, err := encode(in, 99, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 30,
		99,
		100,
		0xab, 0xcd,
		0, 6,
		'f', 'o', 'o', 'b', 'a', 'r',
		4,
		't', 'e', 's', 't',
		1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 6, 7, 8,
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("encode:\n\twant=%v,\n\t got=%v", want, buf)
	}

	out := TestMessage{
		Buf: make([]byte, len(in.Buf)),
	}
	buf = buf[4:] // skip length (2) message type (1) and tag (1)
	if err := decode(&out, buf); err != nil {
		t.Fatal(err)
	}
	if out.Int != in.Int {
		t.Errorf("%v != %v", out.Int, in.Int)
	}
	if out.Str != in.Str {
		t.Errorf("%v != %v", out.Str, in.Str)
	}
	if !bytes.Equal(out.Buf, in.Buf) {
		t.Errorf("%v != %v", out.Buf, in.Buf)
	}
	if out.Arr != in.Arr {
		t.Errorf("%v != %v", out.Arr, in.Arr)
	}
}

func TestCodecEmpty(t *testing.T) {
	var in struct{}

	buf, err := encode(in, 99, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 2, // message length
		99,  // funcId
		100, // tag
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("encode:\n\twant=%v,\n\t got=%v", want, buf)
	}
}
