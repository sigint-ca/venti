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

type FileReader struct {
	ctx context.Context

	br     BlockReader
	e      *Entry
	scores chan Score
}

func NewFileReader(ctx context.Context, br BlockReader, e *Entry) *FileReader {
	r := FileReader{
		ctx:    ctx,
		br:     br,
		e:      e,
		scores: make(chan Score),
	}
	go r.readBlocks()

	return &r
}

func (r *FileReader) Read(p []byte) (int, error) {
	select {
	case s, ok := <-r.scores:
		if !ok {
			return 0, io.EOF
		}
		return r.br.ReadBlock(r.ctx, s, DataType, p)
	case <-r.ctx.Done():
		// TODO: cleanup
		return 0, r.ctx.Err()
	}
}

func (r *FileReader) readBlocks() {
	if r.e.Depth > 0 {
		var in, out chan Score

		in = make(chan Score)
		// TODO: buffered channel instead of goroutine here?
		go func(in chan Score) {
			in <- r.e.Score
			close(in)
		}(in)
		depth := r.e.Depth
		for depth > 0 {
			out = make(chan Score)
			go func(in, out chan Score, depth int) {
				if err := r.unpackPointerBlocks(in, out, depth); err != nil {
					panic(err) // TODO
				}
				close(out)
			}(in, out, depth)
			in = out
			depth--
		}
		for score := range out {
			r.scores <- score
		}
	} else {
		// single-block file, just send it
		r.scores <- r.e.Score
	}

	// signal EOF
	close(r.scores)

	return
}

func (r *FileReader) unpackPointerBlocks(in, out chan Score, depth int) error {
	t := DataType + BlockType(depth)
	buf := make([]byte, r.e.Psize)
	for score := range in {
		n, err := r.br.ReadBlock(r.ctx, score, t, buf)
		if err != nil {
			return err
		}
		nentries := n / ScoreSize
		for i := 0; i < nentries; i++ {
			out <- unpackScore(buf, i)
		}
	}
	return nil
}

func unpackScore(buf []byte, i int) Score {
	var s Score
	copy(s[:], buf[i*ScoreSize:(i+1)*ScoreSize])
	return s
}

type FileWriter struct {
	ctx context.Context

	bw    BlockWriter
	psize int
	dsize int

	depth    int
	size     int64
	pointers []chan Score
	wg       sync.WaitGroup
}

func NewFileWriter(ctx context.Context, bw BlockWriter, psize, dsize int) *FileWriter {
	if dsize <= 0 {
		panic("bad dsize")
	}
	if psize <= 40 {
		panic("bad psize")
	}

	w := FileWriter{
		ctx:   ctx,
		bw:    bw,
		psize: psize,
		dsize: dsize,
	}
	return &w
}

func (w *FileWriter) Write(p []byte) (int, error) {
	if w.pointers == nil {
		w.pointers = make([]chan Score, 10)
		w.pointers[0] = make(chan Score, 1)
	}

	// write data blocks to venti
	for len(p) > 0 {
		block := p
		if len(block) > w.dsize {
			block = block[:w.dsize]
		}
		p = p[len(block):]
		block = ZeroTruncate(DataType, block)
		score, err := w.bw.WriteBlock(w.ctx, DataType, block)
		if err != nil {
			panic("TODO")
		}

		if w.depth == 0 && len(w.pointers[0]) == 1 {
			w.depth = 1
			w.wg.Add(1)
			w.pointers[1] = make(chan Score, w.depth)
			go w.batchPointers(w.depth)
		}

		w.pointers[0] <- score
	}

	w.size += int64(len(p))
	return len(p), nil
}

func (w *FileWriter) batchPointers(depth int) {
	input, output := w.pointers[depth-1], w.pointers[depth]
	t := DataType + BlockType(depth)
	block := make([]byte, 0, w.psize)
	for score := range input {
		block = append(block, score[:]...)
		if len(block)+ScoreSize > w.psize {
			output <- w.writePointerBlock(block, t, depth)
			block = block[:0]
		}
	}

	if len(block) > 0 {
		output <- w.writePointerBlock(block, t, depth)
	}

	close(w.pointers[depth])
	w.wg.Done()
}

func (w *FileWriter) writePointerBlock(block []byte, t BlockType, depth int) Score {
	score, err := w.bw.WriteBlock(w.ctx, t, block)
	if err != nil {
		panic("TODO")
	}

	if w.depth == depth && len(w.pointers[depth]) == 1 {
		w.depth++
		w.wg.Add(1)
		w.pointers[w.depth] = make(chan Score, 1)
		go w.batchPointers(w.depth)
	}

	return score
}

func (w *FileWriter) Flush() (*Entry, error) {
	// TODO: check errors

	if w.pointers == nil {
		panic("no writes to flush")
	}

	close(w.pointers[0])
	w.wg.Wait()

	e := Entry{
		Psize: w.psize,
		Dsize: w.dsize,
		Depth: w.depth,
		Size:  w.size,
		Score: <-w.pointers[w.depth],
	}

	w.pointers = nil
	w.depth = 0

	return &e, nil
}
