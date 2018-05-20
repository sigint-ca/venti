package vac

import venti "sigint.ca/venti2"

type Reader struct {
	br   venti.BlockReader
	root venti.Score
}

func NewReader(br venti.BlockReader, root venti.Score) *Reader {
	r := Reader{
		br:   br,
		root: root,
	}
	return &r
}

func (r *Reader) Next() (*Header, error) {
	return nil, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	return 0, nil
}
