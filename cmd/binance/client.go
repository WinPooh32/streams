package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/WinPooh32/streams/storage"

	"github.com/goccy/go-json"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

var errDial = fmt.Errorf("fialed to dial")

type Message struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func fetch(ctx context.Context, store *storage.Leveldb) error {
	streamsURL := fmt.Sprintf("wss://stream.binance.com:9443/stream?streams=%s", strings.Join(store.List(), "/"))

	log.Println("streams url:", streamsURL)

	c, _, err := websocket.Dial(ctx, streamsURL, nil)
	if err != nil {
		return errDial
	}
	defer c.Close(websocket.StatusInternalError, "exit")

	var connErr error

	for {
		var msg Message

		ctxTimeout, cancel := context.WithTimeout(ctx, 3*time.Minute)

		err = wsjson.Read(ctxTimeout, c, &msg)
		if err != nil {
			cancel()
			connErr = fmt.Errorf("ws read: %w", err)
			break
		}
		cancel()

		dbstream, ok := store.Stream(msg.Stream)
		if !ok {
			log.Printf("unknown stream=%s", msg.Stream)
			continue
		}

		var key = []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))

		for i := 0; ; i++ {
			if i > 0 {
				key = append(key, []byte("-"+strconv.Itoa(i))...)
			}
			_, err = dbstream.Get(key)
			if err == storage.ErrNotFound {
				break
			}
			if err != nil {
				log.Printf("get key=%s: %s", string(key), err)
				break
			}
		}

		err = dbstream.Put(key, msg.Data)
		if err != nil {
			log.Printf("put key=%s, value=%s", string(key), string(msg.Data))
			continue
		}
	}

	err = c.Close(websocket.StatusNormalClosure, "")
	if err != nil {
		return fmt.Errorf("ws close: %w", err)
	}

	if connErr != nil {
		return fmt.Errorf("ws connection error: %w", connErr)
	}

	return nil
}
