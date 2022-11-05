package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/WinPooh32/streams/storage"
	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/errgroup"
)

func makeStreams(symbols []string) (streams []string) {
	for _, s := range symbols {
		s = strings.ToLower(s)
		streams = append(streams, []string{s + "@aggTrade", s + "@depth@100ms", s + "@depth"}...)
	}
	return streams
}

func main() {
	log.Println("Start")
	defer log.Println("Exit")

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if len(os.Args) <= 1 {
		log.Println("Symbols are not defined")
		return
	}

	streams := makeStreams(os.Args[1:])
	log.Println("streams:", streams)

	store, err := storage.OpenLeveldbStorage(streams, "binance.d")
	if err != nil {
		log.Printf("OpenLeveldbStorage: %s", err)
		return
	}

	router := chi.NewRouter()

	router.HandleFunc("/ws/{stream}", serve(store))

	httpServer := http.Server{
		Addr:           ":9443",
		Handler:        router,
		MaxHeaderBytes: 20480,
	}

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		log.Println("http: ListenAndServe")
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: listen and serve: %w", err)
		}
		return nil
	})

	wg.Go(func() error {
		const (
			timeoutHTTP = 60 * time.Second
		)

		<-ctx.Done()

		ctxWithTimeout, cancel := context.WithTimeout(context.Background(), timeoutHTTP)
		defer cancel()

		err = httpServer.Shutdown(ctxWithTimeout)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: shutdown: %w", err)
		}

		return nil
	})

	wg.Go(func() (err error) {
		const delay = 60 * time.Second
		for i := 0; i < 10; i++ {
			startTime := time.Now()

			err = fetch(ctx, store)
			if err != nil {
				log.Printf("fetch events: %s", err)
			}

			if time.Since(startTime) > 2*delay {
				i = 0
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		return nil
	})

	err = wg.Wait()
	if err != nil && err != http.ErrServerClosed {
		log.Printf("fail: %s", err)
		return
	}
}
