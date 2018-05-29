package vac

import (
	"context"
	"errors"

	venti "sigint.ca/venti2"
)

type Reader struct {
	br   venti.BlockReader
	root *File
}

func OpenVac(ctx context.Context, br venti.BlockReader, root *venti.Root) (*Reader, error) {
	if root.Type != "vac" {
		return nil, errors.New("root does not refer to a vac tree")
	}

	f, err := ReadRoot(ctx, br, root)
	if err != nil {
		return nil, err
	}

	r := Reader{
		br:   br,
		root: f,
	}

	return &r, nil
}

func (r *Reader) Next() (*DirEntry, error) {
	return nil, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	return 0, nil
}
