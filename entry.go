package venti

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	EntrySize = 40

	// flags
	EntryActive     uint8 = 1 << 0
	EntryDir        uint8 = 1 << 1
	EntryDepthShift uint8 = 2
	EntryDepthMask  uint8 = 7 << 2
	EntryLocal      uint8 = 1 << 5
	EntryBig        uint8 = 1 << 6
	EntryNoArchive  uint8 = 1 << 7
)

// TODO: methods and private
type Entry struct {
	Gen   int
	Psize int
	Dsize int
	Type  BlockType
	Flags uint8
	Size  int64
	Score Score
}

func (e Entry) Depth() int {
	return int(e.Type & typeDepthMask)
}

func (e Entry) BaseType() BlockType {
	return e.Type & typeBaseMask
}

func (e Entry) IsDir() bool {
	return e.BaseType() == DirType
}

func (e Entry) Pack(p []byte) error {
	if len(p) < EntrySize {
		return errors.New("bad entry size")
	}
	w := bytes.NewBuffer(p[:0])

	flags := e.Flags &^ (EntryDir | EntryDepthMask)
	flags |= uint8(e.Depth()) << EntryDepthShift
	if e.Type-BlockType(e.Depth()) == DirType {
		flags |= EntryDir
	}
	binary.Write(w, binary.BigEndian, uint32(e.Gen))
	pshort := uint16(e.Psize)
	dshort := uint16(e.Dsize)
	if e.Psize > math.MaxUint16 || e.Dsize > math.MaxUint16 {
		flags |= EntryBig
		var err error
		if pshort, err = intToBig(e.Psize); err != nil {
			return err
		}
		if dshort, err = intToBig(e.Dsize); err != nil {
			return err
		}
	}
	binary.Write(w, binary.BigEndian, pshort)
	binary.Write(w, binary.BigEndian, dshort)
	w.WriteByte(flags)
	w.Write(make([]byte, 5))
	writeUint48(w, uint64(e.Size))
	w.Write(e.Score.Bytes())

	if w.Len() != EntrySize {
		panic("invariant failed")
	}

	return nil
}

func UnpackEntry(p []byte) (Entry, error) {
	if len(p) != EntrySize {
		return Entry{}, errors.New("bad entry size")
	}
	var e Entry
	r := bytes.NewReader(p)

	var gen uint32
	binary.Read(r, binary.BigEndian, &gen)
	e.Gen = int(gen)
	var psize, dsize uint16
	binary.Read(r, binary.BigEndian, &psize)
	e.Psize = int(psize)
	binary.Read(r, binary.BigEndian, &dsize)
	e.Dsize = int(dsize)
	e.Flags, _ = r.ReadByte()
	if e.Flags&EntryBig != 0 {
		e.Psize = bigToInt(psize)
		e.Dsize = bigToInt(dsize)
	}
	if e.Flags&EntryDir != 0 {
		e.Type = DirType
	} else {
		e.Type = DataType
	}
	depth := (e.Flags & EntryDepthMask) >> EntryDepthShift
	e.Type += BlockType(depth)
	e.Flags &= ^(EntryDir | EntryDepthMask | EntryBig)
	r.Seek(5, io.SeekCurrent) // skip
	size, _ := readUint48(r)
	e.Size = int64(size)
	ReadScore(&e.Score, r)
	if e.Flags&EntryActive == 0 {
		return e, nil
	}

	if err := checkBlockSize(e.Psize); err != nil {
		return Entry{}, err
	}
	if err := checkBlockSize(e.Dsize); err != nil {
		return Entry{}, err
	}

	if r.Len() > 0 {
		panic(fmt.Sprintf("bytes remaining: %d", r.Len()))
	}

	return e, nil
}

func ReadEntry(r io.Reader) (Entry, error) {
	buf := make([]byte, EntrySize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Entry{}, err
	}
	return UnpackEntry(buf)
}

func checkBlockSize(n int) error {
	if n < 256 {
		return fmt.Errorf("bad block size: %d", n)
	}
	return nil
}
