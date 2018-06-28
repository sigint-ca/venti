package vac

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	venti "sigint.ca/venti2"
)

const DirMagic = 0x1c4d9072

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

const (
	VacDirVersion    = 8
	FossilDirVersion = 9
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

func (dir *DirEntry) Pack(mb *MetaBlock, me MetaEntry, version int) error {
	p := mb.slice(me)
	if version < 8 || version > 9 {
		return fmt.Errorf("bad version %d in (*DirEntry).Pack", version)
	}

	size := len(p)
	w := bytes.NewBuffer(p[:0])

	writeUint32(w, DirMagic)
	writeUint16(w, uint16(version)) // version

	writeString(w, dir.Elem)

	writeUint32(w, uint32(dir.Entry))

	if version == 9 {
		writeUint32(w, uint32(dir.Gen))
		writeUint32(w, uint32(dir.Mentry))
		writeUint32(w, uint32(dir.Mgen))
	}

	writeUint64(w, dir.Qid)

	writeString(w, dir.Uid)
	writeString(w, dir.Gid)
	writeString(w, dir.Mid)

	writeUint32(w, uint32(dir.Mtime.Unix()))
	writeUint32(w, uint32(dir.Mcount))
	writeUint32(w, uint32(dir.Ctime.Unix()))
	writeUint32(w, uint32(dir.Atime.Unix()))
	writeUint32(w, dir.Mode)

	if dir.plan9 && version < 9 {
		w.WriteByte(DirPlan9Entry)
		writeUint16(w, 8+4)
		writeUint64(w, dir.p9Path)
		writeUint32(w, uint32(dir.p9Version))
	}

	if dir.qidSpace {
		w.WriteByte(DirQidSpaceEntry)
		writeUint16(w, 2*8)
		writeUint64(w, dir.qidOffset)
		writeUint64(w, dir.qidMax)
	}

	if dir.Gen != 0 && version < 9 {
		w.WriteByte(DirGenEntry)
		writeUint16(w, 4)
		writeUint32(w, uint32(dir.Gen))
	}

	if w.Len() != size {
		panic("invariant failed")
	}

	return nil
}

func (dir *DirEntry) PackedSize(version int) (int, error) {
	if version < 8 || version > 9 {
		return 0, fmt.Errorf("bad version %d in vdpack", version)
	}

	// constant part
	n := 4 + // magic
		2 + // version
		4 + // entry
		8 + // qid
		4 + // mtime
		4 + // mcount
		4 + // ctime
		4 + // atime
		4 + // mode
		0

	if version == 9 {
		n += 4 + // gen
			4 + // mentry
			4 + // mgen
			0
	}

	// strings
	n += 2 + len(dir.Elem)
	n += 2 + len(dir.Uid)
	n += 2 + len(dir.Gid)
	n += 2 + len(dir.Mid)

	// optional sections
	if version < 9 && dir.plan9 {
		n += 3 + // option header
			8 + // path
			4 // version
	}
	if dir.qidSpace {
		n += 3 + // option header
			8 + // qid offset
			8 + // qid max
			0
	}
	if version < 9 && dir.Gen != 0 {
		n += 3 + // option header
			4 + // gen
			0
	}

	return n, nil
}

type DirScanner struct {
	f *File
	r *venti.SourceReader

	mb   *MetaBlock
	mbuf []byte
	i    int

	de  *DirEntry
	err error
}

func NewDirScanner(ctx context.Context, br venti.BlockReader, f *File) *DirScanner {
	ds := DirScanner{
		f:    f,
		r:    venti.NewReader(ctx, br, f.msource),
		mbuf: make([]byte, f.msource.Dsize),
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
	for ds.mb == nil {
		n, err := ds.r.Read(ds.mbuf)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read meta block: %v", err)
		}

		// TODO: why are there zero-sized meta blocks???
		if n == 0 {
			continue
		}

		memset(ds.mbuf[n:], 0)
		mb, err := UnpackMetaBlock(ds.mbuf)
		if err != nil {
			return nil, fmt.Errorf("unpack meta block: %v", err)
		}
		ds.mb = mb
		break
	}
	me, err := ds.mb.unpackMetaEntry(ds.i)
	if err != nil {
		return nil, fmt.Errorf("unpack meta entry: %v", err)
	}
	de, err := ds.mb.unpackDirEntry(me)
	if err != nil {
		return nil, fmt.Errorf("unpack dir entry: %v", err)
	}

	ds.i++
	return de, nil
}

type DirWriter struct {
	bsize   int
	source  *venti.SourceWriter
	msource *venti.SourceWriter
	mb      *MetaBlock
	i       int
}

func NewDirWriter(ctx context.Context, bw venti.BlockWriter, bsize int) *DirWriter {
	psize := (bsize / venti.ScoreSize) * venti.ScoreSize

	dw := DirWriter{
		bsize:   bsize,
		source:  venti.NewWriter(ctx, bw, venti.DirType, psize, bsize),
		msource: venti.NewWriter(ctx, bw, venti.DataType, psize, bsize),
	}

	return &dw
}

func (dw *DirWriter) Add(f *File) error {
	buf := make([]byte, venti.EntrySize)
	if err := f.source.Pack(buf); err != nil {
		return err
	}
	if _, err := dw.source.Write(buf); err != nil {
		return err
	}
	f.meta.Entry = dw.i
	dw.i++

	if f.IsDir() {
		if err := f.source.Pack(buf); err != nil {
			return err
		}
		if _, err := dw.source.Write(buf); err != nil {
			return err
		}
		f.meta.Mentry = dw.i
		dw.i++
	}

	n, _ := f.meta.PackedSize(VacDirVersion)
	mb := dw.mb
	if mb == nil {
		mb = NewMetaBlock(make([]byte, dw.bsize), dw.bsize/BytesPerEntry)
		dw.mb = mb
	} else {
		nn := (len(dw.mb.buf) * FullPercentage / 100) - dw.mb.size + dw.mb.free
		if n > nn || dw.mb.nIndex == dw.mb.maxIndex {
			if _, err := dw.msource.Write(dw.mb.Pack()); err != nil {
				return err
			}
			mb = NewMetaBlock(make([]byte, dw.bsize), dw.bsize/BytesPerEntry)
			dw.mb = mb
		}
	}

	off, err := mb.Alloc(n)
	if err != nil {
		return err
	}
	found, i, me, err := mb.Search(f.meta.Elem)
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("file already exists: %v", f.meta.Elem)
	}
	me.Offset = off
	me.Size = n
	if err := f.meta.Pack(mb, me, VacDirVersion); err != nil {
		return err
	}
	mb.Insert(i, me)

	return nil
}

func (dw *DirWriter) Close(meta *DirEntry) (*File, error) {
	source, err := dw.source.Flush()
	if err != nil {
		return nil, err
	}

	if dw.mb != nil {
		if _, err := dw.msource.Write(dw.mb.Pack()); err != nil {
			return nil, err
		}
	}
	msource, err := dw.msource.Flush()
	if err != nil {
		return nil, err
	}

	f := File{
		meta:    meta,
		source:  source,
		msource: msource,
	}
	return &f, nil
}
