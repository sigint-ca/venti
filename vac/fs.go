package vac

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	venti "sigint.ca/venti2"
)

func ReadRoot(ctx context.Context, br venti.BlockReader, root *venti.Root) (*File, error) {
	entryBuf := make([]byte, 3*venti.EntrySize)
	n, err := br.ReadBlock(ctx, root.Score, venti.DirType, entryBuf)
	if err != nil {
		return nil, fmt.Errorf("read root venti directory: %v", err)
	}
	if n != 3*venti.EntrySize {
		return nil, errors.New("bad root venti directory size")
	}

	r := bytes.NewReader(entryBuf)
	source, err := venti.ReadEntry(r)
	if err != nil {
		return nil, err
	}
	msource, err := venti.ReadEntry(r)
	if err != nil {
		return nil, err
	}
	rmeta, err := venti.ReadEntry(r)
	if err != nil {
		return nil, err
	}

	var metaBuf bytes.Buffer
	if n, err := venti.NewReader(ctx, br, rmeta).WriteTo(&metaBuf); err != nil {
		return nil, fmt.Errorf("read root meta block: %v (read %d)", err, n)
	}
	mb, err := UnpackMetaBlock(metaBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("unpack root meta block: %v", err)
	}
	me, err := mb.unpackMetaEntry(0)
	if err != nil {
		return nil, fmt.Errorf("unpack root meta entry: %v", err)
	}

	meta, err := mb.unpackDirEntry(me)
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

func WriteRoot(ctx context.Context, bw venti.BlockWriter, dir *File) (venti.Score, error) {
	dsize, psize := dir.source.Dsize, dir.source.Psize

	// root dir block
	buf := make([]byte, 3*venti.EntrySize)

	// source
	if err := dir.source.Pack(buf[0*venti.EntrySize:]); err != nil {
		return venti.Score{}, err
	}

	// msource
	if err := dir.msource.Pack(buf[1*venti.EntrySize:]); err != nil {
		return venti.Score{}, err
	}

	// meta block
	mb := NewMetaBlock(make([]byte, dsize), dsize/BytesPerEntry)
	n, _ := dir.meta.PackedSize(VacDirVersion)
	off, err := mb.Alloc(n)
	if err != nil {
		return venti.Score{}, err
	}
	me := MetaEntry{
		Offset: off,
		Size:   n,
	}
	if err := dir.meta.Pack(mb, me, VacDirVersion); err != nil {
		return venti.Score{}, err
	}
	mb.Insert(0, me)
	mscore, err := bw.WriteBlock(ctx, venti.DataType, mb.Pack())
	if err != nil {
		return venti.Score{}, err
	}
	// meta block entry
	mentry := venti.Entry{
		Psize: psize,
		Dsize: dsize,
		Type:  venti.DataType,
		Flags: venti.EntryActive,
		Size:  int64(dsize),
		Score: mscore,
	}
	if err := mentry.Pack(buf[2*venti.EntrySize:]); err != nil {
		return venti.Score{}, err
	}

	// write root dir block to venti
	score, err := bw.WriteBlock(ctx, venti.DirType, buf)
	if err != nil {
		return venti.Score{}, err
	}

	// root block
	root := venti.Root{
		Name:      "vac",
		Type:      "vac",
		Score:     score,
		BlockSize: dsize,
	}

	buf = make([]byte, venti.RootSize)
	if err := root.Pack(buf); err != nil {
		return venti.Score{}, err
	}

	return bw.WriteBlock(ctx, venti.RootType, buf)
}
