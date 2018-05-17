package venti

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"sigint.ca/venti2/rpc"
)

var testAddr = fmt.Sprintf(":%d", VentiPort)

func startTestServer() {
	backend := MemBackend(make(map[Score][]byte))
	srv, err := NewServer(backend)
	if err != nil {
		panic(err)
	}
	go func() {
		if err := srv.Listen(testAddr); err != nil {
			panic(err)
		}
	}()
	time.Sleep(10 * time.Millisecond)
}

func TestBadRequestType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		t.Fatalf("dial venti: %v", err)
	}

	var req, res struct{}
	if err := client.rpc.Call(ctx, 0, req, &res); err == nil {
		t.Error("expected error")
	} else if _, ok := err.(rpc.ServerError); !ok {
		if ctx.Err() == nil {
			t.Errorf("%v (unexpected)", err)
		} else {
			// no response from server: this is p9p venti's behavior
			// for unknown request types
			t.Logf("%v (expected)", err)
		}
	}

	// connection already closed; should error
	if err := client.Close(); err == nil {
		t.Error("expected error")
	}
}

func TestReadUnknownScore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		t.Fatalf("dial venti: %v", err)
	}

	req := readRequest{
		Score: Fingerprint([]byte("does not exist in venti")),
		Type:  DataType,
		Count: 6,
	}
	res := readResponse{
		Data: make([]byte, 6),
	}
	if err := client.rpc.Call(ctx, rpcRead, req, &res); err == nil {
		t.Error("expected error")
	} else if _, ok := err.(rpc.ServerError); !ok {
		t.Errorf("%v (unexpected)", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("%v (unexpected)", err)
	}
}

func TestPing(t *testing.T) {
	ctx := context.Background()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		t.Fatalf("dial venti: %v", err)
	}

	t.Run("group", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("ping thread %d", i)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				for i := 0; i < 1000; i++ {
					if err := client.Ping(ctx); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	})

	if err := client.Close(); err != nil {
		t.Error(err)
	}
}

func TestReadWrite(t *testing.T) {
	ctx := context.Background()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		t.Fatalf("dial venti: %v", err)
	}

	block := []byte("the quick brown fox jumps over the lazy dog.")
	score := Fingerprint(block)

	s, err := client.WriteBlock(ctx, DataType, block)
	if err != nil {
		t.Fatalf("write block: %v", err)
	}
	if s != score {
		t.Errorf("scores do not match: got=%v, want=%v", s, score)
	}

	dst := make([]byte, len(block))
	if err := client.ReadBlock(ctx, dst, DataType, s); err != nil {
		t.Fatalf("read block: %v", err)
	}
	if !bytes.Equal(dst, block) {
		t.Errorf("read block:\n\twant=%q,\n\t got=%q", block, dst)
	}

	if err := client.Close(); err != nil {
		t.Error(err)
	}
}

func TestSync(t *testing.T) {
	ctx := context.Background()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		t.Fatalf("dial venti: %v", err)
	}

	t.Run("group", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("sync thread %d", i)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				for i := 0; i < 1000; i++ {
					if err := client.Sync(ctx); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	})

	if err := client.Close(); err != nil {
		t.Error(err)
	}
}

func BenchmarkPing(b *testing.B) {
	ctx := context.Background()

	client, err := Dial(ctx, testAddr)
	if err != nil {
		b.Fatalf("dial venti: %v", err)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Ping(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	if err := client.Close(); err != nil {
		b.Error(err)
	}
}
