package venti

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// intToBig converts n to a venti "big" 16 bit floating point
// integer: (n>>5) << (n&31). If n cannot be expressed as a
// whole number in this format, return an error.
func intToBig(n int) (uint16, error) {
	if n > math.MaxUint32 {
		return 0, fmt.Errorf("invalid psize/dsize: %#x", n)
	}
	l := uint32(n)
	var shift uint32
	for l >= (1 << (16 - 5)) {
		if l&1 != 0 {
			return 0, fmt.Errorf("invalid psize/dsize: %#x", n)
		}
		shift++
		l >>= 1
	}

	l = (l << 5) | shift
	if int((l>>5)<<(l&31)) != n {
		return 0, fmt.Errorf("failed to convert to big: %#x => %#x", n, l)
	}
	return uint16(l), nil
}

func bigToInt(n uint16) int {
	return int((n >> 5) << (n & 31))
}

func readUint48(r io.Reader) (uint64, error) {
	var left uint16
	if err := binary.Read(r, binary.BigEndian, &left); err != nil {
		return 0, err
	}
	var right uint32
	if err := binary.Read(r, binary.BigEndian, &right); err != nil {
		return 0, err
	}
	return uint64(left)<<32 | uint64(right), nil
}

func writeUint48(w io.Writer, v uint64) error {
	if err := binary.Write(w, binary.BigEndian, uint16(v>>32)); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, uint32(v))
}
