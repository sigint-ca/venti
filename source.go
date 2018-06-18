package venti

import (
	"context"
	"io"
	"sync"
)

const (
	// Default size of venti data blocks
	DefaultDataSize = 8 * 1024

	// Default size of venti pointer blocks
	DefaultPointerSize = DefaultDataSize - (DefaultDataSize % ScoreSize)
)

type SourceReader struct {
	ctx context.Context

	br BlockReader
	e  Entry

	scores chan *Score
	buf    []byte
	off    int
	end    int
}

func NewReader(ctx context.Context, br BlockReader, e Entry) *SourceReader {
	r := SourceReader{
		ctx:    ctx,
		br:     br,
		e:      e,
		scores: make(chan *Score),
		buf:    make([]byte, e.Dsize),
	}
	go r.readBlocks()

	return &r
}

func (r *SourceReader) Read(p []byte) (int, error) {
	if r.off != r.end {
		// bytes already buffered from last venti read
		n := copy(p, r.buf[r.off:r.end])
		r.off += n
		return n, nil
	}

	// fetch block from venti
	select {
	case s, ok := <-r.scores:
		if !ok {
			return 0, io.EOF
		}
		n, err := r.br.ReadBlock(r.ctx, *s, r.e.BaseType(), r.buf)
		if err != nil {
			return 0, err
		}
		r.off = 0
		r.end = n
	case <-r.ctx.Done():
		// TODO: cleanup
		return 0, r.ctx.Err()
	}

	// copy some or all of the block into p
	n := copy(p, r.buf[r.off:r.end])
	r.off += n
	return n, nil
}

// ReadFrom copys blocks from r to w, using the configured blocksize.
func (r *SourceReader) WriteTo(w io.Writer) (written int64, err error) {
	buf := make([]byte, r.e.Dsize)
	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func (r SourceReader) Seek(off int64, whence int) (int64, error) {
	panic("TODO")
}

func (r *SourceReader) readBlocks() {
	depth := r.e.Depth()
	if depth > 0 {
		var in, out chan *Score

		in = make(chan *Score)
		// TODO: buffered channel instead of goroutine here?
		go func(in chan *Score) {
			in <- &r.e.Score
			close(in)
		}(in)
		for i := 0; i < depth; i++ {
			t := r.e.Type - BlockType(i)
			out = make(chan *Score)
			go func(in, out chan *Score, t BlockType) {
				if err := r.unpackPointerBlocks(in, out, t); err != nil {
					panic(err) // TODO
				}
				close(out)
			}(in, out, t)
			in = out
		}
		for score := range out {
			r.scores <- score
		}
	} else {
		// single-block source, just send it
		r.scores <- &r.e.Score
	}

	// signal EOF
	close(r.scores)

	return
}

func (r *SourceReader) unpackPointerBlocks(in, out chan *Score, t BlockType) error {
	buf := make([]byte, r.e.Psize)
	for score := range in {
		n, err := r.br.ReadBlock(r.ctx, *score, t, buf)
		if err != nil {
			return err
		}
		nentries := n / ScoreSize
		for i := 0; i < nentries; i++ {
			s := unpackScore(buf, i)
			out <- &s
		}
	}
	return nil
}

func unpackScore(buf []byte, i int) Score {
	var s Score
	copy(s.Bytes(), buf[i*ScoreSize:(i+1)*ScoreSize])
	return s
}

type SourceWriter struct {
	ctx context.Context

	bw       BlockWriter
	psize    int
	dsize    int
	baseType BlockType

	depth    int
	size     int64
	pointers []chan *Score
	wg       sync.WaitGroup
}

func NewWriter(ctx context.Context, bw BlockWriter, t BlockType, psize, dsize int) *SourceWriter {
	if dsize <= 0 {
		panic("bad dsize")
	}
	if psize <= 40 {
		panic("bad psize")
	}
	if t != DataType && t != DirType {
		panic("bad type")
	}

	w := SourceWriter{
		ctx:      ctx,
		bw:       bw,
		psize:    psize,
		dsize:    dsize,
		baseType: t,
	}

	return &w
}

// Write adds a block of size len(p) to the current source.
func (w *SourceWriter) Write(p []byte) (int, error) {
	if w.pointers == nil {
		// clean state; this is a new or flushed writer.
		w.pointers = make([]chan *Score, 10)
		w.pointers[0] = make(chan *Score, 1)
	}

	// write data blocks to venti and pass resulting scores
	// to a pointer block writer goroutine.
	o := p
	for len(p) > 0 {
		block := p
		if len(block) > w.dsize {
			block = block[:w.dsize]
		}
		p = p[len(block):]

		// TODO: is this the right place to do this? it seems to mess up the
		// nwritten return value and w.size.
		block = ZeroTruncate(w.baseType, block)

		s := w.writeBlock(block, w.baseType, 0)
		w.pointers[0] <- &s
	}

	w.size += int64(len(o))
	return len(o), nil
}

// ReadFrom copys blocks from r to w, using the configured blocksize.
func (w *SourceWriter) ReadFrom(r io.Reader) (read int64, err error) {
	buf := make([]byte, w.dsize)
	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			read += int64(nr)
			nw, ew := w.Write(buf[0:nr])
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return read, err
}

func (w *SourceWriter) writeBlock(block []byte, t BlockType, depth int) Score {
	score, err := w.bw.WriteBlock(w.ctx, t, block)
	if err != nil {
		panic("TODO")
	}

	if w.depth == depth && len(w.pointers[depth]) == 1 {
		w.depth++
		w.wg.Add(1)
		w.pointers[w.depth] = make(chan *Score, 1)
		go w.batchPointers(w.depth)
	}

	return score
}

func (w *SourceWriter) batchPointers(depth int) {
	input, output := w.pointers[depth-1], w.pointers[depth]
	t := w.baseType + BlockType(depth)
	block := make([]byte, 0, w.psize)
	for score := range input {
		block = append(block, score.Bytes()...)
		if len(block)+ScoreSize > w.psize {
			s := w.writeBlock(block, t, depth)
			output <- &s
			block = block[:0]
		}
	}

	if len(block) > 0 {
		s := w.writeBlock(block, t, depth)
		output <- &s
	}

	close(w.pointers[depth])
	w.wg.Done()
}

// Flush finishes writing the current source, and returns
// and Entry describing it.
func (w *SourceWriter) Flush() (Entry, error) {
	// TODO: check errors

	if w.pointers == nil {
		panic("no writes to flush")
	}

	close(w.pointers[0])
	w.wg.Wait()

	e := Entry{
		Psize: w.psize,
		Dsize: w.dsize,
		Type:  w.baseType + BlockType(w.depth),
		Flags: EntryActive,
		Size:  w.size,
		Score: *<-w.pointers[w.depth],
	}

	w.pointers = nil
	w.depth = 0
	w.size = 0

	return e, nil
}
