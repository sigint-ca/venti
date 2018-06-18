package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"

	venti "sigint.ca/venti2"
	"sigint.ca/venti2/vac"
)

var (
	blocksize = flag.String("b", "8k", "Specifies  the `blocksize` that data will be broken into."+
		"The size must be in the range of 512 bytes to 52k.")
	verboseMode = flag.Bool("v", false, "Print file names as they are added to the archive.")

	bsize, psize int
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("vac: ")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: vac [options] path ...")
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	n, err := parseSize(*blocksize)
	if err != nil {
		log.Fatal(err)
	}
	if n < 512 || n > 52*1024 {
		log.Fatalf("blocksize must be between 512 and 52k")
	}
	bsize = int(n)
	psize = bsize - (bsize % venti.ScoreSize)
	paths := flag.Args()

	ctx := context.Background()
	ctx, cancel := withSignals(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := venti.Dial(ctx, ":17034")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	score, err := vacPaths(ctx, client, paths)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("vac:%v\n", &score)
}

func vacPaths(ctx context.Context, bw venti.BlockWriter, paths []string) (venti.Score, error) {
	var rootVfs []*vac.File
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return venti.Score{}, err
		}
		fi, err := f.Stat()
		if err != nil {
			return venti.Score{}, err
		}

		vprintf("%s\n", path)

		var vf *vac.File
		if fi.IsDir() {
			vf, err = vacDir(ctx, bw, path, f, fi)
		} else {
			meta := vac.FileInfoDirEntry(fi)
			vf, err = vac.NewFile(ctx, bw, f, meta, bsize)
		}
		if err != nil {
			return venti.Score{}, err
		}

		f.Close()
		rootVfs = append(rootVfs, vf)
	}

	// root vac directory containing all paths
	vf, err := vacRoot(ctx, bw, rootVfs)
	if err != nil {
		return venti.Score{}, err
	}

	return vac.WriteRoot(ctx, bw, vf)
}

func vacDir(ctx context.Context, bw venti.BlockWriter, path string, dir *os.File, fi os.FileInfo) (*vac.File, error) {
	w := vac.NewDirWriter(ctx, bw, bsize)
	for {
		base, err := dir.Readdirnames(1)
		if err == io.EOF {
			break
		}
		path := filepath.Join(path, base[0])

		if err != nil {
			return nil, err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			return nil, err
		}

		vprintf("%s\n", path)

		var vf *vac.File
		if fi.IsDir() {
			vf, err = vacDir(ctx, bw, path, f, fi)
		} else {
			meta := vac.FileInfoDirEntry(fi)
			vf, err = vac.NewFile(ctx, bw, f, meta, bsize)
		}
		if err != nil {
			return nil, err
		}

		f.Close()
		if err := w.Add(vf); err != nil {
			return nil, err
		}
	}

	meta := vac.FileInfoDirEntry(fi)
	return w.Close(meta)
}

func vacRoot(ctx context.Context, bw venti.BlockWriter, vfs []*vac.File) (*vac.File, error) {
	meta := vac.DirEntry{
		Elem: "/",
		Mode: 0777 | vac.ModeDir,
		Uid:  "vac",
		Gid:  "vac",
	}

	w := vac.NewDirWriter(ctx, bw, bsize)

	for _, f := range vfs {
		if err := w.Add(f); err != nil {
			return nil, err
		}
	}
	return w.Close(&meta)
}

func vprintf(format string, args ...interface{}) {
	if *verboseMode {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}
