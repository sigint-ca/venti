package vac

// goals: two types of operations: those that need a blockreader
// and context (can we combine these..?) (ReadXXX and WriteXXX)
// and those that operate on data already in mememory. The ones that
// read and write should do minimal other work so that the contexts are
// limited to actual network requests.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	venti "sigint.ca/venti2"
)

type File struct {
	meta    *DirEntry     // metadata for this file
	source  *venti.Source // actual data
	msource *venti.Source // metadata for children in a directory
}

func ReadRoot(ctx context.Context, br venti.BlockReader, root *venti.Root) (*File, error) {
	buf := make([]byte, 3*venti.EntrySize)
	n, err := br.ReadBlock(ctx, root.Score, venti.DirType, buf)
	if err != nil {
		return nil, fmt.Errorf("read root venti directory: %v", err)
	}
	if n != 3*venti.EntrySize {
		return nil, errors.New("bad root venti directory size")
	}

	r := bytes.NewReader(buf)
	e1, err := venti.ReadEntry(r)
	if err != nil {
		return nil, err
	}
	e2, err := venti.ReadEntry(r)
	if err != nil {
		return nil, err
	}
	e3, err := venti.ReadEntry(r)
	if err != nil {
		return nil, err
	}

	// root dir and meta sources
	source, err := venti.SourceReader(ctx, br, e1).ReadSource()
	if err != nil {
		return nil, fmt.Errorf("read root source: %v", err)
	}
	msource, err := venti.SourceReader(ctx, br, e2).ReadSource()
	if err != nil {
		return nil, fmt.Errorf("read root meta source: %v", err)
	}

	// metadata of root source
	rmeta, err := venti.SourceReader(ctx, br, e3).ReadSource()
	if err != nil {
		return nil, fmt.Errorf("read root meta block: %v", err)
	}

	mb, err := ReadMetaBlock(rmeta)
	if err != nil {
		return nil, fmt.Errorf("unpack root meta block: %v", err)
	}
	me, err := mb.unpackMetaEntry(0)
	if err != nil {
		return nil, fmt.Errorf("unpack root meta entry: %v", err)
	}
	meta, err := me.unpackDirEntry()
	if err != nil {
		return nil, fmt.Errorf("unpack root meta data")
	}

	f := File{
		meta:    meta,
		source:  source,
		msource: msource,
	}

	return &f, nil
}

// TODO: document f.source offset after the operation
func (f *File) Walk(ctx context.Context, br venti.BlockReader, de *DirEntry) (*File, error) {
	if !f.IsDir() {
		return nil, errNotDir
	}

	off := int64(de.Entry) * venti.EntrySize
	if _, err := f.source.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	e, err := venti.ReadEntry(f.source)
	if err != nil {
		return nil, err
	}

	source, err := venti.SourceReader(ctx, br, e).ReadSource()
	if err != nil {
		return nil, fmt.Errorf("read file source: %v", err)
	}

	ff := File{
		meta:   de,
		source: source,
	}

	if e.IsDir() {
		off := int64(de.Mentry) * venti.EntrySize
		if _, err := f.source.Seek(off, io.SeekStart); err != nil {
			return nil, err
		}
		ee, err := venti.ReadEntry(f.source)
		if err != nil {
			return nil, err
		}
		msource, err := venti.SourceReader(ctx, br, ee).ReadSource()
		if err != nil {
			return nil, fmt.Errorf("read meta source: %v", err)
		}
		ff.msource = msource
	}

	return &ff, nil
}

func (f *File) DirLookup(elem string) (*DirEntry, error) {
	for {
		mb, err := ReadMetaBlock(f.msource)
		if err == io.EOF {
			return nil, EntryNotFound
		} else if err != nil {
			return nil, err
		}
		me, err := mb.search(elem)
		if err == EntryNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		return me.unpackDirEntry()
	}
}

func (f *File) IsDir() bool {
	return f.meta.Mode&ModeDir != 0
}

func (f *File) Reader() *venti.Source {
	return f.source
}
