package venti

import (
	"bytes"
	"fmt"
)

// ZeroExtend pads buf with zeros or zero scores, according to
// the given type, reslicing it from size to newsize bytes.
// The capacity of buf must be at least newsize.
func ZeroExtend(t BlockType, buf []byte, size, newsize int) error {
	if newsize > cap(buf) {
		return fmt.Errorf("newsize is too large for buffer")
	}
	buf = buf[:newsize]

	if t.depth() > 0 {
		start := (size / ScoreSize) * ScoreSize
		end := (newsize / ScoreSize) * ScoreSize
		var i int
		for i = start; i < end; i += ScoreSize {
			s := ZeroScore()
			copy(buf[i:], s.Bytes())
		}
		memset(buf[i:], 0)
	} else {
		memset(buf[size:], 0)
	}
	return nil
}

func memset(p []byte, c byte) {
	for i := 0; i < len(p); i++ {
		p[i] = c
	}
}

// ZeroTruncate returns a new slice of buf which excludes
// trailing zeros or zero scores, according to the type.
func ZeroTruncate(t BlockType, buf []byte) []byte {
	if t.depth() > 0 {
		// ignore slop at end of block
		i := (len(buf) / ScoreSize) * ScoreSize
		zero := ZeroScore()
		zeroBytes := zero.Bytes()
		for i >= ScoreSize {
			if bytes.Equal(buf[i-ScoreSize:i], zeroBytes) {
				break
			}
			i -= ScoreSize
		}
		return buf[:i]
	} else if t == RootType {
		if len(buf) < RootSize {
			return buf
		}
		return buf[:RootSize]
	} else {
		var i int
		for i = len(buf); i > 0; i-- {
			if buf[i-1] != 0 {
				break
			}
		}
		return buf[:i]
	}
}
