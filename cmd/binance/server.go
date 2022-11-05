package main

import (
	"bufio"
	"context"
	"log"
	"net/http"
	"time"

	"github.com/WinPooh32/streams/storage"
	"github.com/go-chi/chi/v5"
	"nhooyr.io/websocket"
)

func serve(store *storage.Leveldb) http.HandlerFunc {
	fn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns:  []string{"*"},
			CompressionMode: websocket.CompressionDisabled,
			// CompressionMode:      websocket.CompressionContextTakeover,
			// CompressionThreshold: 64,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer c.Close(websocket.StatusInternalError, "bye bye")

		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Hour)
		defer cancel()

		streamName := chi.URLParam(r, "stream")

		stream, ok := store.Stream(streamName)
		if !ok {
			log.Printf("serve: unknown stream=%s", streamName)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		reader, err := stream.Read()
		if err != nil {
			log.Printf("stream: read: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		scanner := bufio.NewScanner(&reader)
		for {
			var value []byte
			// read key.
			if ok := scanner.Scan(); !ok {
				break
			}
			// read value.
			if ok := scanner.Scan(); !ok {
				break
			}
			value = scanner.Bytes()

			err = c.Write(ctx, websocket.MessageText, value)
			if err != nil {
				log.Printf("ws: write: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("scan: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		c.Close(websocket.StatusNormalClosure, "done")
	})
	return fn
}
