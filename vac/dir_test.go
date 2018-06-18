package vac

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	venti "sigint.ca/venti2"
)

func TestDirIO(t *testing.T) {
	ctx := context.Background()

	client, err := venti.Dial(ctx, ":17034")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var score venti.Score
	if t.Run("write", func(t *testing.T) {
		score = testWriteDir(t, ctx, client)
	}) {
		t.Run("scan", func(t *testing.T) {
			testScanDir(t, ctx, client, score)
		})
	}
}

func testWriteDir(t *testing.T, ctx context.Context, bw venti.BlockWriter) venti.Score {
	bsize := 1024
	w := NewDirWriter(ctx, bw, bsize)

	for i := 0; i < 5; i++ {
		r := strings.NewReader(fmt.Sprintf("foo %d", i))
		de := DirEntry{Elem: fmt.Sprintf("f%d", i)}
		f, err := NewFile(ctx, bw, r, &de, bsize)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Add(f); err != nil {
			t.Fatal(f)
		}
	}
	meta := DirEntry{Elem: "test_dir", Mode: 0644 ^ ModeDir}
	dir, err := w.Close(&meta)
	if err != nil {
		t.Fatal(err)
	}

	score, err := WriteRoot(ctx, bw, dir)
	if err != nil {
		t.Fatal(err)
	}
	return score
}

func testScanDir(t *testing.T, ctx context.Context, br venti.BlockReader, score venti.Score) {
	buf := make([]byte, venti.RootSize)
	if _, err := br.ReadBlock(ctx, score, venti.RootType, buf); err != nil {
		t.Fatalf("read root: %v", err)
	}

	root, err := venti.UnpackRoot(buf)
	if err != nil {
		t.Fatal(err)
	}

	f, err := ReadRoot(ctx, br, root)
	if err != nil {
		t.Fatal(err)
	}

	scanner := NewDirScanner(ctx, br, f)
	for scanner.Scan() {
		de := scanner.DirEntry()
		f, err := f.Walk(ctx, br, de)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if _, err := f.Reader(ctx, br).WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %q", de.Elem, buf.String())
	}
	if err := scanner.Err(); err != nil {
		t.Error(err)
	}
}
