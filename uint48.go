package venti

import (
	"encoding/binary"
	"io"
)

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
