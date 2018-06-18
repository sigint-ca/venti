package vac

// goals: two types of operations: those that need a blockreader
// and context (can we combine these..?) (ReadXXX and WriteXXX)
// and those that operate on data already in mememory. The ones that
// read and write should do minimal other work so that the contexts are
// limited to actual network requests.

import (
	"bytes"
	"context"
	"io"

	venti "sigint.ca/venti2"
)

type File struct {
	meta *DirEntry // metadata for this file

	// TODO: rename these
	source  venti.Entry // actual data
	msource venti.Entry // metadata for children in a directory
}

func NewFile(ctx context.Context, bw venti.BlockWriter, r io.Reader, meta *DirEntry, bsize int) (*File, error) {
	psize := (bsize / venti.ScoreSize) * venti.ScoreSize
	sw := venti.NewWriter(ctx, bw, venti.DataType, psize, bsize)
	if _, err := sw.ReadFrom(r); err != nil {
		return nil, err
	}
	e, err := sw.Flush()
	if err != nil {
		return nil, err
	}
	f := File{
		meta:   meta,
		source: e,
	}
	return &f, nil
}

func (f *File) Name() string {
	return f.meta.Elem
}

// TODO: document f.source offset after the operation
func (f *File) Walk(ctx context.Context, br venti.BlockReader, de *DirEntry) (*File, error) {
	if !f.IsDir() {
		return nil, errNotDir
	}

	// TODO: seek without reading the whole source into memory
	var buf bytes.Buffer
	_, err := venti.NewReader(ctx, br, f.source).WriteTo(&buf)
	r := bytes.NewReader(buf.Bytes())

	off := int64(de.Entry) * venti.EntrySize
	if _, err := r.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	e, err := venti.ReadEntry(r)
	if err != nil {
		return nil, err
	}

	ff := File{
		meta:   de,
		source: e,
	}

	if e.IsDir() {
		off := int64(de.Mentry) * venti.EntrySize
		if _, err := r.Seek(off, io.SeekStart); err != nil {
			return nil, err
		}
		ee, err := venti.ReadEntry(r)
		if err != nil {
			return nil, err
		}
		ff.msource = ee
	}

	return &ff, nil
}

// TODO: shouldn't need to pass in br
func (f *File) DirLookup(ctx context.Context, br venti.BlockReader, elem string) (*DirEntry, error) {
	r := venti.NewReader(ctx, br, f.msource)
	buf := make([]byte, f.msource.Dsize)
	for {
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}
		mb, err := UnpackMetaBlock(buf)
		if err == io.EOF {
			return nil, EntryNotFound
		} else if err != nil {
			return nil, err
		}
		found, _, me, err := mb.Search(elem)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}

		return mb.unpackDirEntry(me)
	}
}

func (f *File) IsDir() bool {
	return f.meta.Mode&ModeDir != 0
}

func (f *File) Reader(ctx context.Context, br venti.BlockReader) *venti.SourceReader {
	return venti.NewReader(ctx, br, f.source)
}
