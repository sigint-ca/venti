package vac

import "time"

type Header struct {
	Name  string
	Uid   string
	Gid   string
	Mid   string
	Mtime time.Time
	Ctime time.Time
	Atime time.Time
	Mode  int64
}
