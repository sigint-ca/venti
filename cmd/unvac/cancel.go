package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
)

func withSignals(ctx context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	ctx, cancel0 := context.WithCancel(ctx)

	c := make(chan os.Signal)
	signal.Notify(c, signals...)

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			signal.Stop(c)
			cancel0()
			close(c)
		})
	}

	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}
