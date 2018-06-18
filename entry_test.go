package venti

import (
	"testing"
)

func TestPackEntry(t *testing.T) {
	e := Entry{
		Gen:   1,
		Psize: 8192,
		Dsize: 8192,
		Type:  DataType,
		Flags: EntryLocal,
		Score: ZeroScore(),
	}

	buf := make([]byte, EntrySize)
	if err := e.Pack(buf); err != nil {
		t.Fatal(err)
	}

	ee, err := UnpackEntry(buf)
	if err != nil {
		t.Fatal(err)
	}

	if ee != e {
		t.Fatalf("results differ: \n%v\n\tvs\n%v", e, ee)
	}
}

var PackedEntrySink []byte

func BenchmarkPackEntry(b *testing.B) {
	e := Entry{
		Gen:   1,
		Psize: 8192,
		Dsize: 8192,
		Type:  DataType,
		Flags: EntryLocal,
		Score: ZeroScore(),
	}

	PackedEntrySink = make([]byte, EntrySize)
	for i := 0; i < b.N; i++ {
		if err := e.Pack(PackedEntrySink); err != nil {
			b.Fatal(err)
		}
	}
}

var UnpackedEntrySink Entry

func BenchmarkUnpackEntry(b *testing.B) {
	e := Entry{
		Gen:   1,
		Psize: 8192,
		Dsize: 8192,
		Type:  DataType,
		Flags: EntryLocal,
		Score: ZeroScore(),
	}

	buf := make([]byte, EntrySize)
	if err := e.Pack(buf); err != nil {
		b.Fatal(err)
	}

	var err error
	for i := 0; i < b.N; i++ {
		UnpackedEntrySink, err = UnpackEntry(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
