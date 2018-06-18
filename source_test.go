package venti

import (
	"bytes"
	"context"
	"testing"
)

func TestSourceIO(t *testing.T) {
	ctx := context.Background()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		t.Fatalf("dial venti: %v", err)
	}

	w := NewWriter(ctx, client, DataType, 3*ScoreSize, 20)

	type test struct {
		s     []byte
		depth int
	}
	for _, test := range []test{
		{[]byte("foobar"), 0},
		{[]byte("this is 2 blocks and 1 pointer"), 1},
		{[]byte("this tree has five data blocks, two pointers of type DataType+1, and one DataType+2"), 2},
	} {
		if _, err := w.Write(test.s); err != nil {
			t.Error(err)
		}
		e, err := w.Flush()
		if err != nil {
			t.Error(err)
		}
		if e.Depth() != test.depth {
			t.Errorf("bad depth: got %d, want %d", e.Depth(), test.depth)
		}
		if e.Size != int64(len(test.s)) {
			t.Errorf("bad size: got %d, want %d", e.Size, len(test.s))
		}
		t.Logf("flush returned entry with depth=%d", e.Depth())

		var w bytes.Buffer
		if _, err := NewReader(ctx, client, e).WriteTo(&w); err != nil {
			t.Fatal(err)
		}
		buf := w.Bytes()
		t.Logf("read source: %q", buf)
		if !bytes.Equal(buf, test.s) {
			t.Errorf("read: got %q, want %q", buf, test.s)
		}
	}

	if err := client.Close(); err != nil {
		t.Error(err)
	}
}
