package venti

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

const (
	entrySize = 40

	// flags
	entryActive     uint8 = 1 << 0
	entryDir        uint8 = 1 << 1
	entryDepthShift uint8 = 2
	entryDepthMask  uint8 = 7 << 2
	entryLocal      uint8 = 1 << 5
	entryBig        uint8 = 1 << 6
	entryNoArchive  uint8 = 1 << 7
)

type Entry struct {
	Gen   int
	Psize int
	Dsize int
	Type  BlockType
	Flags uint8
	Size  int64
	Score Score
}

// entryBig integer format is floating-point:
// (n>>5) << (n&31).
// Convert this number; must be exact or return -1.
func intToBig(n int) (uint16, error) {
	if n > math.MaxUint32 {
		return 0, fmt.Errorf("invalid entry psize/dsize: %x", n)
	}
	l := uint32(n)
	var shift uint32
	for l >= (1 << (16 - 5)) {
		if l&1 != 0 {
			return 0, fmt.Errorf("invalid entry psize/dsize: %x", n)
		}
		shift++
		l >>= 1
	}

	l = (l << 5) | shift
	if int((l>>5)<<(l&31)) != n {
		return 0, fmt.Errorf("failed to convert to big: %x => %x", n, l)
	}
	return uint16(l), nil
}

func bigToInt(n uint16) int {
	return int((n >> 5) << (n & 31))
}

func checkBlockSize(n int) error {
	if n < 256 {
		return fmt.Errorf("bad block size %x", n)
	}
	return nil
}

func (e *Entry) Pack(p []byte) error {
	w := bytes.NewBuffer(p[:0])
	flags := e.Flags &^ (entryDir | entryDepthMask)
	flags |= uint8(e.Depth()) << entryDepthShift
	if e.Type-BlockType(e.Depth()) == DirType {
		flags |= entryDir
	}
	binary.Write(w, binary.BigEndian, uint32(e.Gen))
	pshort := uint16(e.Psize)
	dshort := uint16(e.Dsize)
	if e.Psize > math.MaxUint16 || e.Dsize > math.MaxUint16 {
		flags |= entryBig
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
	return nil
}

func UnpackEntry(p []byte) (*Entry, error) {
	var e Entry
	buf := bytes.NewBuffer(p)
	var gen uint32
	binary.Read(buf, binary.BigEndian, &gen)
	e.Gen = int(gen)
	var psize, dsize uint16
	binary.Read(buf, binary.BigEndian, &psize)
	e.Psize = int(psize)
	binary.Read(buf, binary.BigEndian, &dsize)
	e.Dsize = int(dsize)
	e.Flags, _ = buf.ReadByte()
	if e.Flags&entryBig != 0 {
		e.Psize = bigToInt(psize)
		e.Dsize = bigToInt(dsize)
	}
	if e.Flags&entryDir != 0 {
		e.Type = DirType
	} else {
		e.Type = DataType
	}
	depth := (e.Flags & entryDepthMask) >> entryDepthShift
	e.Type += BlockType(depth)
	e.Flags &= ^(entryDir | entryDepthMask | entryBig)
	buf.Next(5) // skip
	size, _ := readUint48(buf)
	e.Size = int64(size)
	score, err := ReadScore(buf)
	if err != nil {
		return nil, err
	}
	e.Score = score
	if e.Flags&entryActive == 0 {
		return &e, nil
	}
	if err := checkBlockSize(e.Psize); err != nil {
		return nil, err
	}
	if err := checkBlockSize(e.Dsize); err != nil {
		return nil, err
	}
	return &e, nil
}

func (e *Entry) Depth() int {
	return int(e.Type & typeDepthMask)
}

func (e *Entry) BaseType() BlockType {
	return e.Type & typeBaseMask
}
