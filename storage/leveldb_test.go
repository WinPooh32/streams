package storage

import (
	"bufio"
	"bytes"
	"math/rand"
	"testing"
)

func genline(size int) (line []byte) {
	line = make([]byte, size)
	for j := range line {
		const p = 65
		const m = 90 - p
		line[j] = byte(rand.Intn(m) + p)
	}
	return line
}

func genTestData(total, sizeKey, sizeData int) (data map[string][]byte) {
	data = make(map[string][]byte, total)
	for i := 0; i < total; i++ {
		data[string(genline(sizeKey))] = genline(sizeData)
	}
	return data
}

func TestLeveldbStorage(t *testing.T) {
	var (
		key1   = []byte("ключ_1")
		value1 = []byte("привет мир!")
	)
	type args struct {
		streams []string
		data    map[string][]byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "simple",
			args: args{
				streams: []string{"a", "b", "c"},
				data:    genTestData(100500, 16, 512),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			t.Log(dir)

			gotStorage, err := OpenLeveldbStorage(tt.args.streams, dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenLeveldbStorage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			defer func() {
				err = gotStorage.Close()
				if err != nil {
					t.Errorf("close storage: %s", err)
					return
				}
			}()

			var touched = make(map[string]bool, len(tt.args.data))

			for _, name := range tt.args.streams {
				for key := range tt.args.data {
					touched[string(key)] = false
				}

				stream, ok := gotStorage.Stream(name)
				if !ok {
					t.Errorf("required stream=%s is not presented in the stream", name)
					return
				}
				for key, value := range tt.args.data {
					stream.Put([]byte(key), value)
					if err != nil {
						t.Errorf("put value to stream=%s: %s", name, err)
						return
					}
				}
				r, err := stream.Read()
				if err != nil {
					t.Errorf("open reader of the stream=%s: %s", name, err)
					return
				}

				scanner := bufio.NewScanner(&r)
				for {
					var (
						key   []byte
						value []byte
					)

					if ok := scanner.Scan(); !ok {
						break
					}
					key = scanner.Bytes()

					if ok := scanner.Scan(); !ok {
						break
					}
					value = scanner.Bytes()

					if testData := tt.args.data[string(key)]; !bytes.Equal(value, testData) {
						t.Errorf("\ngot key=%s\n   value=%s,\nbut want=%s", key, value, testData)
						return
					}

					touched[string(key)] = true
				}
				if err := scanner.Err(); err != nil {
					t.Errorf("full scan of the stream=%s: %s", name, err)
					return
				}

				for key := range tt.args.data {
					if !touched[string(key)] {
						t.Errorf("key=%s of the stream=%s was not read!", key, name)
						return
					}
				}
			}

			for _, name := range tt.args.streams {
				stream, ok := gotStorage.Stream(name)
				if !ok {
					t.Errorf("required stream=%s is not presented in the stream", name)
					return
				}
				err = stream.Put(key1, value1)
				if err != nil {
					t.Errorf("put value to stream=%s: %s", name, err)
					continue
				}
				value, err := stream.Get(key1)
				if err != nil {
					t.Errorf("get value from stream=%s by the key=%s: %s", name, value, err)
					continue
				}
				if !bytes.Equal(value, value1) {
					t.Errorf("got value=%s, but want=%s", value, value1)
					continue
				}
			}
		})
	}
}
