package ventirpc

import (
	"encoding/binary"
	"fmt"
	"io"
)

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

func readSmall(r io.Reader) ([]byte, error) {
	// read buffer length
	var n uint8
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}

	// read bytes
	buf := make([]byte, n)
	nn, err := r.Read(buf)
	if err != nil {
		return nil, err
	}
	if nn != int(n) {
		return nil, fmt.Errorf("short read")
	}
	return buf, nil
}

func writeSmall(w io.Writer, s []byte) error {
	// write string length
	n := uint8(len(s))
	if err := binary.Write(w, binary.BigEndian, n); err != nil {
		return err
	}

	// write string
	_, err := w.Write(s)
	return err
}
