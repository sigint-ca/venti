package venti

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"
)

func TestFileWriter(t *testing.T) {
	ctx := context.Background()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		t.Fatalf("dial venti: %v", err)
	}

	w := NewFileWriter(ctx, client, DataType, 3*ScoreSize, 20)

	type test struct {
		block []byte
		depth int
	}
	for _, test := range []test{
		{[]byte("foobar"), 0},
		{[]byte("this is 2 blocks and 1 pointer"), 1},
		{[]byte("this tree has five data blocks, two pointers of type DataType+1, and one DataType+2"), 2},
	} {
		if _, err := w.Write(test.block); err != nil {
			t.Error(err)
		}
		e, err := w.Flush()
		if err != nil {
			t.Error(err)
		}
		if e.Depth() != test.depth {
			t.Errorf("bad depth: got %d, want %d", e.Depth(), test.depth)
		}
		t.Logf("flush returned score=%v", e.Score)

		r := NewFileReader(ctx, client, e)
		buf, err := ioutil.ReadAll(r)
		t.Logf("read file: %q", buf)
		if !bytes.Equal(buf, test.block) {
			t.Errorf("read: got %q, want %q", buf, test.block)
		}
	}

	if err := client.Close(); err != nil {
		t.Error(err)
	}
}
