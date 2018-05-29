package vac

import (
	"errors"
	"io"
)

type DirScanner struct {
	f *File

	mb *MetaBlock
	i  int

	de  *DirEntry
	err error
}

func NewDirScanner(f *File) *DirScanner {
	ds := DirScanner{
		f: f,
	}
	if !f.IsDir() {
		ds.err = errors.New("not a directory")
	}
	return &ds
}

func (ds *DirScanner) Scan() bool {
	de, err := ds.next()
	if err != nil && ds.err == nil {
		ds.err = err
	}
	ds.de = de
	return ds.err == nil
}

func (ds *DirScanner) DirEntry() *DirEntry {
	return ds.de
}

func (ds *DirScanner) Err() error {
	if ds.err == io.EOF {
		return nil
	}
	return ds.err
}

func (ds *DirScanner) next() (*DirEntry, error) {
	if ds.mb != nil && ds.i >= ds.mb.nIndex {
		ds.mb = nil
		ds.i = 0
	}
	if ds.mb == nil {
		mb, err := ReadMetaBlock(ds.f.msource)
		if err != nil {
			return nil, err
		}
		ds.mb = mb
	}
	me, err := ds.mb.unpackMetaEntry(ds.i)
	if err != nil {
		return nil, err
	}
	de, err := me.unpackDirEntry()
	if err != nil {
		return nil, err
	}

	ds.i++
	return de, nil
}
