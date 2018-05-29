package main

import (
	"context"
	"fmt"
	"log"
	"os"

	venti "sigint.ca/venti2"
	"sigint.ca/venti2/vac"
)

func main() {
	ctx := context.Background()

	client, err := venti.Dial(ctx, ":17034")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	score, err := venti.ParseScore("791a8f463e30c210f9a9150d20316152b41dcbc8")
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, venti.RootSize)
	client.ReadBlock(ctx, score, venti.RootType, buf)

	root, err := venti.UnpackRoot(buf)
	if err != nil {
		log.Fatal(err)
	}

	f, err := vac.ReadRoot(ctx, client, root)
	if err != nil {
		log.Fatal(err)
	}

	scanner := vac.NewDirScanner(f)
	for scanner.Scan() {
		e := scanner.DirEntry()
		if err := writeFile(ctx, client, f, e); err != nil {
			log.Print(err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Print(err)
	}
}

func writeFile(ctx context.Context, br venti.BlockReader, dir *vac.File, e *vac.DirEntry) error {
	f, err := dir.Walk(ctx, br, e)
	if err != nil {
		return err
	}
	dest, err := os.Create(fmt.Sprintf("%s", e.Elem))
	if err != nil {
		return err
	}
	defer dest.Close()
	if _, err := f.Reader().WriteTo(dest); err != nil {
		return err
	}
	return nil
}
