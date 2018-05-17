package venti

import (
	"crypto/sha1"
	"fmt"
)

const ScoreSize = sha1.Size

type Score [ScoreSize]byte

func Fingerprint(data []byte) Score {
	return sha1.Sum(data)
}

func (s Score) String() string {
	return fmt.Sprintf("%x", [ScoreSize]byte(s))
}
