package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"

	venti "sigint.ca/venti2"
	"sigint.ca/venti2/vac"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("unvac: ")

	flag.Parse()
	score, err := venti.ParseScore(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	ctx, cancel := withSignals(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := venti.Dial(ctx, ":17034")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	buf := make([]byte, venti.RootSize)
	if _, err := client.ReadBlock(ctx, score, venti.RootType, buf); err != nil {
		log.Fatal(err)
	}

	root, err := venti.UnpackRoot(buf)
	if err != nil {
		log.Fatal(err)
	}

	f, err := vac.ReadRoot(ctx, client, root)
	if err != nil {
		log.Fatal(err)
	}

	if err := unvacDir(ctx, client, "", f); err != nil {
		log.Fatal(err)
	}
}

func unvacDir(ctx context.Context, br venti.BlockReader, dir string, f *vac.File) error {
	scanner := vac.NewDirScanner(ctx, br, f)
	for scanner.Scan() {
		e := scanner.DirEntry()
		ff, err := f.Walk(ctx, br, e)
		if err != nil {
			return fmt.Errorf("walk: %v", err)
		}
		if e.Mode&vac.ModeDir != 0 {
			dir := filepath.Join(dir, e.Elem)
			if err := os.Mkdir(dir, 0777); err != nil {
				return err
			}
			if err := unvacDir(ctx, br, dir, ff); err != nil {
				return err
			}
		} else if err := writeFile(ctx, br, dir, ff, e); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan: %v", err)
	}

	return nil
}

func writeFile(ctx context.Context, br venti.BlockReader, dir string, f *vac.File, meta *vac.DirEntry) error {
	dest, err := os.Create(filepath.Join(dir, meta.Elem))
	if err != nil {
		return err
	}
	// TODO: set file metadata

	defer dest.Close()
	if _, err := f.Reader(ctx, br).WriteTo(dest); err != nil {
		return err
	}
	return nil
}
