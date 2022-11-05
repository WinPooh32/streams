package main

import (
	"context"
	"encoding/csv"
	"os"
	"os/signal"
	"strconv"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// WsAggTradeEvent define websocket aggregate trade event
type WsAggTradeEvent struct {
	Event                 string `json:"e"`
	Time                  int64  `json:"E"`
	Symbol                string `json:"s"`
	AggTradeID            int64  `json:"a"`
	Price                 string `json:"p"`
	Quantity              string `json:"q"`
	FirstBreakdownTradeID int64  `json:"f"`
	LastBreakdownTradeID  int64  `json:"l"`
	TradeTime             int64  `json:"T"`
	IsBuyerMaker          bool   `json:"m"`
	Placeholder           bool   `json:"M"` // add this field to avoid case insensitive unmarshaling
}

func main() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	connectCtx, connectCancel := context.WithTimeout(ctx, time.Minute)
	defer connectCancel()

	c, _, err := websocket.Dial(connectCtx, "ws://127.0.0.1:9443/ws/btcusdt@aggTrade", &websocket.DialOptions{
		// CompressionMode:      websocket.CompressionContextTakeover,
		// CompressionThreshold: 64,
	})
	if err != nil {
		panic(err)
	}
	defer c.Close(websocket.StatusInternalError, "the sky is falling")
	connectCancel()

	w := csv.NewWriter(os.Stdout)
	w.Comma = ','
	w.UseCRLF = false
	defer w.Flush()

	record := []string{
		"Event",
		"Time",
		"Symbol",
		"AggTradeID",
		"Price",
		"Quantity",
		"FirstBreakdownTradeID",
		"LastBreakdownTradeID",
		"TradeTime",
		"IsBuyerMaker",
	}

	err = w.Write(record)
	if err != nil {
		panic(err)
	}

	for {
		var msg WsAggTradeEvent

		ctxRead, readCancel := context.WithTimeout(ctx, 10*time.Minute)

		err = wsjson.Read(ctxRead, c, &msg)
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			break
		}
		if err != nil {
			readCancel()
			panic(err)
		}
		readCancel()

		record[0] = msg.Event
		record[1] = strconv.FormatInt(msg.Time, 10)
		record[2] = msg.Symbol
		record[3] = strconv.FormatInt(msg.AggTradeID, 10)
		record[4] = msg.Price
		record[5] = msg.Quantity
		record[6] = strconv.FormatInt(msg.FirstBreakdownTradeID, 10)
		record[7] = strconv.FormatInt(msg.LastBreakdownTradeID, 10)
		record[8] = strconv.FormatInt(msg.TradeTime, 10)
		record[9] = strconv.FormatBool(msg.IsBuyerMaker)

		err = w.Write(record)
		if err != nil {
			panic(err)
		}
	}

	c.Close(websocket.StatusNormalClosure, "good bye")
}
