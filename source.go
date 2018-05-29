package venti

import (
	"bytes"
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

type Source struct {
	*bytes.Reader
	buf   []byte
	bsize int
}

func (s *Source) Bytes() []byte {
	return s.buf
}

func (s *Source) BlockSize() int {
	return s.bsize
}

type BlockReader interface {
	// ReadBlock reads the block with the given score and type into buf,
	// whose length determines the maximum size of the block, and returns
	// the number of bytes read.
	ReadBlock(ctx context.Context, s Score, t BlockType, buf []byte) (int, error)
}

type BlockWriter interface {
	// WriteBlock writes the contents of buf as a block of the given
	// type, returning the score.
	WriteBlock(ctx context.Context, t BlockType, buf []byte) (Score, error)
}

type sourceReader struct {
	ctx context.Context

	br BlockReader
	e  *Entry

	scores chan *Score
	buf    []byte
	off    int
	end    int
}

func SourceReader(ctx context.Context, br BlockReader, e *Entry) *sourceReader {
	r := sourceReader{
		ctx:    ctx,
		br:     br,
		e:      e,
		scores: make(chan *Score),
		buf:    make([]byte, e.Dsize),
	}
	go r.readBlocks()

	return &r
}

func (r *sourceReader) ReadSource() (*Source, error) {
	w := bytes.NewBuffer(make([]byte, 0, r.e.Size))
	n, err := w.ReadFrom(r)
	if err != nil {
		return nil, err
	}

	// TODO: should we check this?
	//if n != r.e.Size {
	//	return nil, fmt.Errorf("short read: read %d, entry wants %d", n, r.e.Size)
	//}
	_ = n

	buf := w.Bytes()
	s := Source{
		Reader: bytes.NewReader(buf),
		buf:    buf,
		bsize:  r.e.Dsize,
	}
	return &s, nil
}

func (r *sourceReader) Read(p []byte) (int, error) {
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

func (r *sourceReader) readBlocks() {
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

func (r *sourceReader) unpackPointerBlocks(in, out chan *Score, t BlockType) error {
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

type sourceWriter struct {
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

func SourceWriter(ctx context.Context, bw BlockWriter, t BlockType, psize, dsize int) *sourceWriter {
	if dsize <= 0 {
		panic("bad dsize")
	}
	if psize <= 40 {
		panic("bad psize")
	}
	if t != DataType && t != DirType {
		panic("bad type")
	}

	w := sourceWriter{
		ctx:      ctx,
		bw:       bw,
		psize:    psize,
		dsize:    dsize,
		baseType: t,
	}

	return &w
}

// TODO: fix block size behaviour
func (w *sourceWriter) Write(p []byte) (int, error) {
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

func (w *sourceWriter) batchPointers(depth int) {
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

func (w *sourceWriter) writeBlock(block []byte, t BlockType, depth int) Score {
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

func (w *sourceWriter) Flush() (*Entry, error) {
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
		Size:  w.size,
		Score: *<-w.pointers[w.depth],
	}

	w.pointers = nil
	w.depth = 0
	w.size = 0

	return &e, nil
}
