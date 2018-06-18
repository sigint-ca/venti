package main

import (
	"os"
	"syscall"
	"time"
)

func FileInfoDirEntry(fi os.FileInfo) *DirEntry {
	sys := fi.Sys().(*syscall.Dir)

	var de DirEntry

	de.Elem = fi.Name()
	de.Size = fi.Size()
	de.Uid = sys.Uid
	de.Gid = sys.Gid
	de.Mtime = time.Unix(int64(sys.Mtime), 0)
	de.Ctime = time.Unix(int64(sys.Mtime), 0)
	de.Atime = time.Unix(int64(sys.Atime, 0)
	de.Mode = uint32(fi.Mode() & 0777)
	if fi.IsDir() {
		de.Mode ^= ModeDir
	}

	return &de
}
