package vac

import "errors"

var (
	EntryNotFound = errors.New("entry not found")

	errBadPath     = errors.New("bad path")
	errCorruptMeta = errors.New("corrupt meta data")
	errNotDir      = errors.New("not a directory")
)
