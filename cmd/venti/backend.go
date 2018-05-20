package venti

import "errors"

type Backend interface {
	ReadBlock(Score, []byte) (int, error)
	WriteBlock(typ uint8, data []byte) (Score, error)
}

var (
	ENotFound = errors.New("block not found")
)

type MemBackend map[Score][]byte

func (b MemBackend) ReadBlock(s Score, p []byte) (int, error) {
	buf, ok := b[s]
	if !ok {
		return 0, ENotFound
	}
	return copy(p, buf), nil
}

func (b MemBackend) WriteBlock(typ uint8, data []byte) (Score, error) {
	s := Fingerprint(data)
	b[s] = data
	return s, nil
}
