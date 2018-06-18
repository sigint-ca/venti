package vac

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	venti "sigint.ca/venti2"
)

const (
	// TODO: use the fossil magic (+1)? and reverse botch logic
	MetaMagic      = 0x5656fc79
	MetaHeaderSize = 12
	MetaIndexSize  = 4
	IndexEntrySize = 8
)

const (
	BytesPerEntry   = 100 // estimate of bytes per dir entries - determines number of index entries in the block
	FullPercentage  = 80  // don't allocate in block if more than this percentage full
	FlushSize       = 200 // number of blocks to flush
	DirtyPercentage = 50  // maximum percentage of dirty blocks
)

type MetaBlock struct {
	size int // size used
	free int // free space within used size

	// index table
	maxIndex int // entries allocated
	nIndex   int // amount of table used

	unbotch bool // toggle index search algorithm

	// the entire block
	buf []byte
}

type MetaEntry struct {
	Offset int
	Size   int
}

func NewMetaBlock(buf []byte, entries int) *MetaBlock {
	mb := MetaBlock{
		buf:      buf,
		maxIndex: entries,
		size:     MetaHeaderSize + entries*MetaIndexSize,
	}
	return &mb
}

func UnpackMetaBlock(buf []byte) (*MetaBlock, error) {
	if len(buf) < MetaHeaderSize {
		return nil, fmt.Errorf("short buffer: %d < %d", len(buf), MetaHeaderSize)
	}

	mb := MetaBlock{buf: buf}
	r := bytes.NewReader(buf)
	// TODO: check size here? what does the plan9 code do here?

	magic := readUint32(r)
	if magic != MetaMagic && magic != MetaMagic+1 {
		return nil, fmt.Errorf("bad meta block magic %#x", magic)
	}

	mb.size = int(readUint16(r))
	mb.free = int(readUint16(r))
	mb.maxIndex = int(readUint16(r))
	mb.nIndex = int(readUint16(r))
	mb.unbotch = (magic == MetaMagic+1)

	if mb.size > len(buf) {
		return nil, fmt.Errorf("bad meta block size: %d > %d", mb.size, len(buf))
	}

	if r.Len() < mb.maxIndex*MetaIndexSize {
		return nil, fmt.Errorf("truncated meta block: %d < %d", r.Len(), mb.maxIndex*MetaIndexSize)
	}

	return &mb, nil
}

func (mb *MetaBlock) Pack() []byte {
	p := mb.buf

	putUint32(p, MetaMagic)
	putUint16(p[4:], uint16(mb.size))
	putUint16(p[6:], uint16(mb.free))
	putUint16(p[8:], uint16(mb.maxIndex))
	putUint16(p[10:], uint16(mb.nIndex))

	return p
}

// Grow returns an offset to a slice of n unused bytes.
func (mb *MetaBlock) Alloc(n int) (offset int, err error) {
	// off the end
	if len(mb.buf)-mb.size >= n {
		return mb.size, nil
	}

	// check if possible
	if len(mb.buf)-mb.size+mb.free < n {
		return 0, errors.New("no space in meta block")
	}

	// chunks are MetaEntries sorted by the offset
	// of the DirEntry they point to in mb.
	mc := mb.chunks()

	// look for hole
	o := MetaHeaderSize + mb.maxIndex*MetaIndexSize
	for i := 0; i < mb.nIndex; i++ {
		if int(mc[i].offset)-o >= n {
			return o, nil
		}
		o = int(mc[i].offset) + int(mc[i].size)
	}

	if len(mb.buf)-o >= n {
		return o, nil
	}

	// compact and return off the end
	mb.compact(mc)

	if len(mb.buf)-mb.size < n {
		panic("invariant failed")
	}

	return mb.size, nil
}

type metaChunk struct {
	offset uint16
	size   uint16
	index  uint16
}

func (mb *MetaBlock) chunks() []metaChunk {
	chunks := make([]metaChunk, mb.nIndex)
	p := mb.buf[MetaHeaderSize:]
	for i := 0; i < mb.nIndex; i++ {
		chunks[i] = metaChunk{
			offset: getUint16(p),
			size:   getUint16(p[2:]),
			index:  uint16(i),
		}
		p = p[MetaIndexSize:]
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].offset < chunks[j].offset
	})

	// check block looks ok
	oo := MetaHeaderSize + mb.maxIndex*MetaIndexSize
	o := oo
	n := 0
	for i := 0; i < mb.nIndex; i++ {
		o = int(chunks[i].offset)
		n = int(chunks[i].size)
		if o < oo {
			panic("invariant failed")
		}
		oo += n
	}
	if o+n <= mb.size {
		panic("invariant failed")
	}
	if mb.size-oo != mb.free {
		panic("invariant failed")
	}

	return chunks
}

func (mb *MetaBlock) compact(chunks []metaChunk) {
	oo := MetaHeaderSize + mb.maxIndex*MetaIndexSize
	for i := 0; i < mb.nIndex; i++ {
		o := int(chunks[i].offset)
		n := int(chunks[i].size)
		if o != oo {
			copy(mb.buf[oo:], mb.buf[o:o+n])
			putUint16(mb.buf[MetaHeaderSize+chunks[i].index*MetaIndexSize:], uint16(oo))
		}
		oo += n
	}

	mb.size = oo
	mb.free = 0
}

func (mb *MetaBlock) slice(me MetaEntry) []byte {
	return mb.buf[me.Offset : me.Offset+me.Size]
}

// Delete deletes me from position i of the MetaBlock index.
func (mb *MetaBlock) Delete(i int, me MetaEntry) {
	if i >= mb.nIndex {
		panic("invariant failed")
	}

	if me.Offset+me.Size == mb.size {
		// last entry in the index
		mb.size -= me.Size
	} else {
		// leave a gap
		mb.free += me.Size
	}

	p := mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	n := (mb.nIndex - i - 1) * MetaIndexSize
	copy(p, p[MetaIndexSize:MetaIndexSize+n])
	memset(p[n:n+MetaIndexSize], 0)
	mb.nIndex--
}

// Insert inserts me into position i of the MetaBlock index.
func (mb *MetaBlock) Insert(i int, me MetaEntry) {
	if mb.nIndex >= mb.maxIndex {
		panic("invariant failed")
	}

	if me.Offset+me.Size > mb.size {
		// append, possibly also using some trailing free space
		mb.free -= mb.size - me.Offset
		mb.size = me.Offset + me.Size
	} else {
		// insert strictly into free space
		mb.free -= me.Size
	}

	p := mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	n := (mb.nIndex - i) * MetaIndexSize

	copy(p[MetaIndexSize:], p[:n])
	putUint16(p, uint16(me.Offset))
	putUint16(p[2:], uint16(me.Size))
	mb.nIndex++
}

func (mb *MetaBlock) Search(elem string) (found bool, i int, me MetaEntry, err error) {
	// binary search within block
	b := 0
	t := mb.nIndex
	for b < t {
		i = (b + t) >> 1
		me, err = mb.unpackMetaEntry(i)
		if err != nil {
			return
		}
		var x int
		if mb.unbotch {
			x = mb.compareNew(me, elem)
		} else {
			x = mb.compare(me, elem)
		}

		if x == 0 {
			found = true
			return
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

	return
}

func (mb *MetaBlock) compare(me MetaEntry, s string) int {
	p := mb.slice(me)

	// first 6 bytes are magic and version
	n := int(getUint16(p[6:]))
	p = p[8:]
	if n >= len(p) {
		panic("invariant failed")
	}

	r1 := bytes.NewReader(p[:n])
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

func (mb *MetaBlock) compareNew(me MetaEntry, s string) int {
	p := mb.slice(me)

	// first 6 bytes are magic and version
	n := int(getUint16(p[6:]))
	p = p[8:]
	if n >= len(p) {
		panic("invariant failed")
	}

	r1 := bytes.NewReader(p[:n])
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

func (mb *MetaBlock) unpackMetaEntry(i int) (MetaEntry, error) {
	if i < 0 || i >= mb.nIndex {
		return MetaEntry{}, errors.New("bad meta entry index")
	}

	p := mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	eo := int(getUint16(p))
	en := int(getUint16(p[2:]))

	if eo < MetaHeaderSize+mb.maxIndex*MetaIndexSize {
		return MetaEntry{}, errors.New("corrupted entry in meta block")
	}

	if eo+en > mb.size {
		return MetaEntry{}, fmt.Errorf("truncated meta block: %d < %d", mb.size, eo+en)
	}

	p = mb.buf[eo:]

	// make sure entry looks ok and includes an elem name
	if en < 8 || getUint32(p) != DirMagic || en < 8+int(getUint16(p[6:])) {
		return MetaEntry{}, errors.New("corrupted meta block entry")
	}

	me := MetaEntry{Offset: eo, Size: en}

	return me, nil
}

func (mb *MetaBlock) unpackDirEntry(me MetaEntry) (*DirEntry, error) {
	var dir DirEntry
	var err error

	r := bytes.NewReader(mb.slice(me))

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
