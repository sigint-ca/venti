package vac

import (
	"encoding/binary"
	"fmt"
	"io"
)

func getUint16(p []byte) uint16 {
	return binary.BigEndian.Uint16(p)
}

func getUint32(p []byte) uint32 {
	return binary.BigEndian.Uint32(p)
}

func getUint64(p []byte) uint64 {
	return binary.BigEndian.Uint64(p)
}

func readUint16(r io.Reader) uint16 {
	var n uint16
	binary.Read(r, binary.BigEndian, &n)
	return n
}

func readUint32(r io.Reader) uint32 {
	var n uint32
	binary.Read(r, binary.BigEndian, &n)
	return n
}

func readUint64(r io.Reader) uint64 {
	var n uint64
	binary.Read(r, binary.BigEndian, &n)
	return n
}

func readString(r io.Reader) (string, error) {
	// read string length
	var n uint16
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", err
	}

	// read string
	buf := make([]byte, n)
	nn, err := r.Read(buf)
	if err != nil {
		return "", err
	}
	if nn != int(n) {
		return "", fmt.Errorf("short read: want %d, read %d", n, nn)
	}
	return string(buf), nil
}

func writeString(w io.Writer, s string) error {
	// write string length
	n := uint16(len(s))
	if err := binary.Write(w, binary.BigEndian, n); err != nil {
		return err
	}

	// write string
	_, err := w.Write([]byte(s))
	return err
}
