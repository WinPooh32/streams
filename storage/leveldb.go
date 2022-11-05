package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"

	"github.com/hashicorp/go-multierror"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var ErrNotFound = errors.New("not found")

type Leveldb struct {
	streams map[string]*leveldb.DB
}

func OpenLeveldbStorage(streams []string, dir string) (storage *Leveldb, err error) {
	storage = &Leveldb{
		streams: make(map[string]*leveldb.DB, len(streams)),
	}
	for _, name := range streams {
		storage.streams[name], err = leveldb.OpenFile(path.Join(dir, name), nil)
		if err != nil {
			return nil, fmt.Errorf("open leveldb database for stream=%s: %w", name, err)
		}
	}
	return storage, nil
}

func (ldb *Leveldb) Stream(name string) (stream LeveldbStream, ok bool) {
	stream.db, ok = ldb.streams[name]
	return
}

func (ldb *Leveldb) List() (list []string) {
	for name := range ldb.streams {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

func (ldb *Leveldb) Close() error {
	var err *multierror.Error
	for _, db := range ldb.streams {
		err = multierror.Append(db.Close())
	}
	return err.ErrorOrNil()
}

type LeveldbStream struct {
	db *leveldb.DB
}

func (stream LeveldbStream) Get(key []byte) (value []byte, err error) {
	value, err = stream.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	return value, err
}

func (stream LeveldbStream) Put(key []byte, value []byte) (err error) {
	return stream.db.Put(key, value, nil)
}

func (stream LeveldbStream) Read() (r LeveldbStreamReader, err error) {
	r.snap, err = stream.db.GetSnapshot()
	if err != nil {
		return r, fmt.Errorf("leveldb: get snapshot: %w", err)
	}
	return r, nil
}

type LeveldbStreamReader struct {
	snap *leveldb.Snapshot
	buf  *bytes.Buffer
	iter iterator.Iterator
	next bool
	eof  bool
}

func (r *LeveldbStreamReader) Range(slice *util.Range) {
	if r.buf != nil {
		r.buf.Reset()
	} else {
		r.buf = bytes.NewBuffer(nil)
	}
	if r.iter != nil {
		r.iter.Release()
	}
	r.iter = r.snap.NewIterator(slice, nil)
	r.next = true
}

func (r *LeveldbStreamReader) Read(p []byte) (n int, err error) {
	if r.eof {
		return 0, io.EOF
	}
	if r.iter == nil {
		r.Range(nil)
	}
	if r.next {
		for r.buf.Len() < len(p) {
			if !r.iter.Next() && r.iter.Error() == nil {
				r.eof = true
				break
			}
			if err := r.iter.Error(); err != nil {
				return 0, err
			}
			r.buf.Write(r.iter.Key())
			r.buf.WriteByte('\n')
			r.buf.Write(r.iter.Value())
			err = r.buf.WriteByte('\n')
			if err != nil {
				return 0, err
			}
		}
		r.next = false
	}
	n, err = r.buf.Read(p)
	if err == io.EOF {
		if r.eof {
			return n, io.EOF
		}
		r.next = true
		return n, nil
	}
	return n, err
}

func (r *LeveldbStreamReader) Seek(key []byte) bool {
	return r.iter.Seek(key)
}

func (r *LeveldbStreamReader) Close() error {
	r.iter.Release()
	r.snap.Release()
	return nil
}
