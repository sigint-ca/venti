package ventirpc

import (
	"bytes"
	"testing"
)

func TestStringEncoding(t *testing.T) {
	var buf bytes.Buffer

	err := writeString(&buf, "foobar")
	if err != nil {
		t.Fatal(err)
	}

	s, err := readString(&buf)
	if err != nil {
		t.Fatalf("failed to unpack string")
	}
	if s != "foobar" {
		t.Errorf("unpacked bad string: got %q, wanted %q", s, "foobar")
	}
}
