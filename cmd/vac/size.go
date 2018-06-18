package main

import (
	"errors"
	"strconv"
)

func parseSize(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("empty size")
	}

	var mul uint64
	switch s[len(s)-1] {
	case 'k', 'K':
		mul = 1024
		s = s[:len(s)-1]
	case 'm', 'M':
		mul = 1024 * 1024
		s = s[:len(s)-1]
	case 'g', 'G':
		mul = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	default:
		mul = 1
	}

	n, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		return 0, err
	}

	return int64(n * mul), nil
}
