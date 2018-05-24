package venti

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
)

const ScoreSize = sha1.Size

type Score [ScoreSize]byte

func ZeroScore() Score {
	return Score{
		0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
		0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
	}
}

func ReadScore(s *Score, r io.Reader) error {
	n, err := r.Read(s[:])
	if err != nil {
		return err
	}
	if n != ScoreSize {
		return errors.New("short read")
	}
	return nil
}

func Fingerprint(data []byte) Score {
	return sha1.Sum(data)
}

func (s *Score) String() string {
	return fmt.Sprintf("%x", [ScoreSize]byte(*s))
}

func (s *Score) Bytes() []byte {
	return s[:]
}
