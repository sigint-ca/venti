package vac

import (
	"os"
	"strconv"
	"syscall"
	"time"
)

func FileInfoDirEntry(fi os.FileInfo) *DirEntry {
	sys := fi.Sys().(*syscall.Stat_t)

	var de DirEntry

	de.Elem = fi.Name()
	de.Size = fi.Size()
	de.Uid = strconv.Itoa(int(sys.Uid))
	de.Gid = strconv.Itoa(int(sys.Gid))
	de.Mtime = time.Unix(sys.Mtimespec.Unix())
	de.Ctime = time.Unix(sys.Ctimespec.Unix())
	de.Atime = time.Unix(sys.Atimespec.Unix())
	de.Mode = uint32(fi.Mode() & 0777)
	if fi.IsDir() {
		de.Mode ^= ModeDir
	}

	return &de
}
