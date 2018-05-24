package venti

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	RootSize       = 300
	rootVersion    = 2
	rootVersionBig = 1 << 15
)

type Root struct {
	Name      string
	Type      string
	Score     Score
	BlockSize int
	Prev      Score
}

func (r *Root) Pack(p []byte) error {
	if len(p) != RootSize {
		return errors.New("bad root size")
	}
	w := bytes.NewBuffer(p[:0])

	vers := uint16(rootVersion)
	bshort := uint16(r.BlockSize)
	if r.BlockSize >= math.MaxUint16 {
		vers |= rootVersionBig
		var err error
		bshort, err = intToBig(r.BlockSize)
		if err != nil {
			return err
		}
	}
	binary.Write(w, binary.BigEndian, vers)
	buf := make([]byte, 128)
	copy(buf, r.Name)
	w.Write(buf)
	memset(buf, 0)
	copy(buf, r.Type)
	w.Write(buf)
	w.Write(r.Score.Bytes())
	binary.Write(w, binary.BigEndian, bshort)
	w.Write(r.Prev.Bytes())

	if w.Len() != RootSize {
		panic(fmt.Sprintf("bad root size: %d", w.Len()))
	}

	return nil
}

func UnpackRoot(p []byte) (*Root, error) {
	if len(p) != RootSize {
		return nil, errors.New("bad root size")
	}
	var root Root
	r := bytes.NewReader(p)

	var vers uint16
	binary.Read(r, binary.BigEndian, &vers)
	if vers&^rootVersionBig != rootVersion {
		return nil, fmt.Errorf("unknown root version: %#x", vers)
	}
	buf := make([]byte, 128)
	r.Read(buf)
	root.Name = string(buf)
	end := strings.IndexByte(root.Name, 0)
	if end >= 0 {
		root.Name = root.Name[:end]
	}
	memset(buf, 0)
	r.Read(buf)
	root.Type = string(buf)
	end = strings.IndexByte(root.Type, 0)
	if end >= 0 {
		root.Type = root.Type[:end]
	}
	ReadScore(&root.Score, r)
	var bshort uint16
	binary.Read(r, binary.BigEndian, &bshort)
	root.BlockSize = int(bshort)
	if vers&rootVersionBig != 0 {
		root.BlockSize = bigToInt(bshort)
	}
	if err := checkBlockSize(root.BlockSize); err != nil {
		return nil, err
	}
	ReadScore(&root.Prev, r)

	if r.Len() > 0 {
		panic(fmt.Sprintf("bytes remaining: %d", r.Len()))
	}

	return &root, nil
}
