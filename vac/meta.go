package vac

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	venti "sigint.ca/venti2"
)

const (
	MetaMagic      = 0x5656fc79
	MetaHeaderSize = 12
	MetaIndexSize  = 4
	IndexEntrySize = 8
	DirMagic       = 0x1c4d9072
)

// Mode bits
const (
	ModeOtherExec  = (1 << 0)
	ModeOtherWrite = (1 << 1)
	ModeOtherRead  = (1 << 2)
	ModeGroupExec  = (1 << 3)
	ModeGroupWrite = (1 << 4)
	ModeGroupRead  = (1 << 5)
	ModeOwnerExec  = (1 << 6)
	ModeOwnerWrite = (1 << 7)
	ModeOwnerRead  = (1 << 8)
	ModeSticky     = (1 << 9)
	ModeSetUid     = (1 << 10)
	ModeSetGid     = (1 << 11)
	ModeAppend     = (1 << 12) // append only file
	ModeExclusive  = (1 << 13) // lock file - plan 9
	ModeLink       = (1 << 14) // sym link
	ModeDir        = (1 << 15) // duplicate of DirEntry
	ModeHidden     = (1 << 16) // MS-DOS
	ModeSystem     = (1 << 17) // MS-DOS
	ModeArchive    = (1 << 18) // MS-DOS
	ModeTemporary  = (1 << 19) // MS-DOS
	ModeSnapshot   = (1 << 20) // read only snapshot
	ModeDevice     = (1 << 21) // Unix device
	ModeNamedPipe  = (1 << 22) // Unix named pipe
)

const (
	DirPlan9Entry    = 1 + iota // not valid in version >= 9
	DirNTEntry                  // not valid in version >= 9
	DirQidSpaceEntry            //
	DirGenEntry                 // not valid in version >= 9
)

type DirEntry struct {
	Elem   string // name (final path element only)
	Entry  int    // entry number for Venti file or directory
	Gen    int    // generation number
	Mentry int    // entry number for Venti file holding metadata
	Mgen   int    // generation number
	Size   int64  // size of file
	Qid    uint64 // unique file serial number

	Uid    string    // owner
	Gid    string    // group
	Mid    string    // last modified by
	Mtime  time.Time // last modification time
	Mcount int       // number of modifications: can wrap!
	Ctime  time.Time // creation time
	Atime  time.Time // last access time
	Mode   uint32    // mode bits

	// plan 9
	plan9     bool
	p9Path    uint64
	p9Version int

	// sub space of qid
	qidSpace  bool
	qidOffset uint64 // qid offset
	qidMax    uint64 // qid maximum
}

type MetaBlock struct {
	size     int // size used
	free     int // free space within used size
	maxIndex int // entries allocated for table
	nIndex   int // amount of table used
	unbotch  bool
	buf      []byte
}

// TODO: this can just be a bytes.Reader?
type MetaEntry struct {
	p    []byte
	size int
}

func ReadMetaBlock(r *venti.Source) (*MetaBlock, error) {
	var mb MetaBlock

	off, _ := r.Seek(0, io.SeekCurrent)

	var magic uint32
	if err := binary.Read(r, binary.BigEndian, &magic); err != nil {
		return nil, err
	}
	if magic != MetaMagic && magic != MetaMagic+1 {
		return nil, fmt.Errorf("bad meta block magic %#x", magic)
	}

	var size, free, maxIndex, nIndex uint16
	binary.Read(r, binary.BigEndian, &size)
	binary.Read(r, binary.BigEndian, &free)
	binary.Read(r, binary.BigEndian, &maxIndex)
	binary.Read(r, binary.BigEndian, &nIndex)
	mb.size = int(size)
	mb.free = int(free)
	mb.maxIndex = int(maxIndex)
	mb.nIndex = int(nIndex)
	mb.unbotch = (magic == MetaMagic+1)

	if mb.size > r.BlockSize() {
		return nil, errors.New("bad meta block size")
	}

	if r.Len() < mb.maxIndex*MetaIndexSize {
		return nil, errors.New("truncated meta block")
	}

	mb.buf = r.Bytes()[off:r.BlockSize()]
	r.Seek(off+int64(r.BlockSize()), io.SeekStart)

	return &mb, nil
}

func (mb *MetaBlock) search(elem string) (*MetaEntry, error) {
	// binary search within block
	b := 0
	t := mb.nIndex
	for b < t {
		i := (b + t) >> 1
		me, err := mb.unpackMetaEntry(i)
		if err != nil {
			return nil, err
		}
		var x int
		if mb.unbotch {
			x = me.compareNew(elem)
		} else {
			x = me.compare(elem)
		}

		if x == 0 {
			return me, nil
		}

		if x < 0 {
			b = i + 1
		} else { // x > 0
			t = i
		}
	}

	if b != t {
		panic("invariant failed")
	}

	return nil, EntryNotFound
}

func (me *MetaEntry) compare(s string) int {
	n := getUint16(me.p[6:me.size])

	if int(n)+8 >= me.size {
		panic("invariant failed")
	}

	r1 := bytes.NewReader(me.p[8:n])
	r2 := strings.NewReader(s)
	for r1.Len() > 0 {
		if r2.Len() == 0 {
			return -1
		}
		c1, _ := r1.ReadByte()
		c2, _ := r2.ReadByte()
		if c1 < c2 {
			return -1
		}
		if c1 > c2 {
			return 1
		}
	}
	if r2.Len() == 0 {
		return 0
	}
	return 1
}

func (me *MetaEntry) compareNew(s string) int {
	n := getUint16(me.p[6:me.size])

	if int(n)+8 >= me.size {
		panic("invariant failed")
	}

	r1 := bytes.NewReader(me.p[8:n])
	r2 := strings.NewReader(s)
	for r1.Len() > 0 {
		if r2.Len() == 0 {
			return 1
		}
		c1, _ := r1.ReadByte()
		c2, _ := r2.ReadByte()
		if c1 < c2 {
			return -1
		}
		if c1 > c2 {
			return 1
		}
	}
	if r2.Len() == 0 {
		return 0
	}
	return -1
}

func (mb *MetaBlock) unpackMetaEntry(i int) (*MetaEntry, error) {
	if i < 0 || i >= mb.nIndex {
		return nil, errors.New("bad meta entry index")
	}

	p := mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	eo := int(getUint16(p))
	en := int(getUint16(p[2:]))

	if eo < MetaHeaderSize+mb.maxIndex*MetaIndexSize {
		return nil, errors.New("corrupted entry in meta block")
	}

	if eo+en > mb.size {
		return nil, errors.New("truncated meta block")
	}

	p = mb.buf[eo:]

	// make sure entry looks ok and includes an elem name
	if en < 8 || getUint32(p) != DirMagic || en < 8+int(getUint16(p[6:])) {
		return nil, errors.New("corrupted meta block entry")
	}

	me := MetaEntry{
		p:    p,
		size: en,
	}

	return &me, nil
}

func (me *MetaEntry) unpackDirEntry() (*DirEntry, error) {
	var dir DirEntry
	var err error

	r := bytes.NewReader(me.p[:me.size])

	// magic
	if r.Len() < 4 {
		return nil, errCorruptMeta
	}
	if readUint32(r) != DirMagic {
		return nil, errCorruptMeta
	}

	// version
	if r.Len() < 2 {
		return nil, errCorruptMeta
	}
	version := readUint16(r)
	if version < 7 || version > 9 {
		return nil, errCorruptMeta
	}

	// elem
	dir.Elem, err = readString(r)
	if err != nil {
		return nil, errCorruptMeta
	}

	// entry
	if r.Len() < 4 {
		return nil, errCorruptMeta
	}
	dir.Entry = int(readUint32(r))

	if version < 9 {
		dir.Gen = 0
		dir.Mentry = dir.Entry + 1
		dir.Mgen = 0
	} else {
		if r.Len() < 3*4 {
			return nil, errCorruptMeta
		}
		dir.Gen = int(readUint32(r))
		dir.Mentry = int(readUint32(r))
		dir.Mgen = int(readUint32(r))
	}

	// size is gotten from DirEntry

	// qid
	if r.Len() < 8 {
		return nil, errCorruptMeta
	}
	dir.Qid = uint64(readUint64(r))

	// skip replacement
	if version == 7 {
		if r.Len() < venti.ScoreSize {
			return nil, errCorruptMeta
		}
		r.Seek(venti.ScoreSize, io.SeekCurrent)
	}

	// uid
	dir.Uid, err = readString(r)
	if err != nil {
		return nil, errCorruptMeta
	}

	// gid
	dir.Gid, err = readString(r)
	if err != nil {
		return nil, errCorruptMeta
	}

	// mid
	dir.Mid, err = readString(r)
	if err != nil {
		return nil, errCorruptMeta
	}

	if r.Len() < 5*4 {
		return nil, errCorruptMeta
	}
	dir.Mtime = time.Unix(int64(readUint32(r)), 0)
	dir.Mcount = int(readUint32(r))
	dir.Ctime = time.Unix(int64(readUint32(r)), 0)
	dir.Atime = time.Unix(int64(readUint32(r)), 0)
	dir.Mode = readUint32(r)

	// optional meta data
	for r.Len() > 0 {
		if r.Len() < 3 {
			return nil, errCorruptMeta
		}
		t, _ := r.ReadByte()
		nn := int(readUint16(r))
		if r.Len() < nn {
			return nil, errCorruptMeta
		}
		switch t {
		case DirPlan9Entry:
			// not valid in version >= 9
			if version >= 9 {
				break
			}
			if dir.plan9 || nn != 12 {
				return nil, errCorruptMeta
			}
			dir.plan9 = true
			dir.p9Path = readUint64(r)
			dir.p9Version = int(readUint32(r))
			if dir.Mcount == 0 {
				dir.Mcount = dir.p9Version
			}
			break
		case DirGenEntry:
			// not valid in version >= 9
			if version >= 9 {
				break
			}
			break
		case DirQidSpaceEntry:
			if dir.qidSpace || nn != 16 {
				return nil, errCorruptMeta
			}
			dir.qidSpace = true
			dir.qidOffset = readUint64(r)
			dir.qidMax = readUint64(r)
			break
		}
	}

	if r.Len() != 0 {
		return nil, errCorruptMeta
	}

	return &dir, nil
}
