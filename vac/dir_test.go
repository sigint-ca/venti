package vac

import (
	"context"
	"testing"

	venti "sigint.ca/venti2"
)

func TestDirIterator(t *testing.T) {
	ctx := context.Background()

	client, err := venti.Dial(ctx, ":17034")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	score, err := venti.ParseScore("791a8f463e30c210f9a9150d20316152b41dcbc8")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, venti.RootSize)
	client.ReadBlock(ctx, score, venti.RootType, buf)

	root, err := venti.UnpackRoot(buf)
	if err != nil {
		t.Fatal(err)
	}

	f, err := ReadRoot(ctx, client, root)
	if err != nil {
		t.Fatal(err)
	}

	scanner := NewDirScanner(f)
	for scanner.Scan() {
		t.Logf("%s", scanner.DirEntry().Elem)
	}
	if err := scanner.Err(); err != nil {
		t.Error(err)
	}
}
