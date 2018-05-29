package venti

import (
	"errors"

	venti "sigint.ca/venti2"
)

type Backend interface {
	ReadBlock(venti.Score, []byte) (int, error)
	WriteBlock(typ uint8, data []byte) (venti.Score, error)
}

var (
	ENotFound = errors.New("block not found")
)

type MemBackend map[venti.Score][]byte

func (b MemBackend) ReadBlock(s venti.Score, p []byte) (int, error) {
	buf, ok := b[s]
	if !ok {
		return 0, ENotFound
	}
	return copy(p, buf), nil
}

func (b MemBackend) WriteBlock(typ uint8, data []byte) (venti.Score, error) {
	s := venti.Fingerprint(data)
	b[s] = data
	return s, nil
}
