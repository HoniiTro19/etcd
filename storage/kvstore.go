package storage

import (
	"bytes"
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/coreos/etcd/storage/backend"
	"github.com/coreos/etcd/storage/storagepb"
)

var (
	batchLimit    = 10000
	batchInterval = 100 * time.Millisecond
	keyBucketName = []byte("key")
)

type store struct {
	mu sync.RWMutex

	b       backend.Backend
	kvindex index

	currentIndex uint64

	subIndex uint32 // tracks next subIndex to put into backend
}

func newStore(path string) *store {
	s := &store{
		b:            backend.New(path, batchInterval, batchLimit),
		kvindex:      newTreeIndex(),
		currentIndex: 0,
	}

	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(keyBucketName)
	tx.Unlock()
	s.b.ForceCommit()

	return s
}

func (s *store) Put(key, value []byte) int64 {
	s.TnxBegin()
	s.put(key, value, s.currentIndex+1)
	s.TnxEnd()

	return int64(s.currentIndex)
}

func (s *store) Range(key, end []byte, limit, rangeIndex int64) (kvs []storagepb.KeyValue, index int64) {
	s.TnxBegin()
	kvs, index = s.rangeKeys(key, end, limit, rangeIndex)
	s.TnxEnd()

	return kvs, index
}

func (s *store) DeleteRange(key, end []byte) (n, index int64) {
	s.TnxBegin()
	n = s.deleteRange(key, end, s.currentIndex+1)
	s.TnxEnd()

	return n, int64(s.currentIndex)
}

func (s *store) TnxBegin() {
	s.mu.Lock()
	s.subIndex = 0
}

func (s *store) TnxEnd() {
	if s.subIndex != 0 {
		s.currentIndex += 1
	}
	s.subIndex = 0
	s.mu.Unlock()
}

func (s *store) TnxRange(key, end []byte, limit, rangeIndex int64) (kvs []storagepb.KeyValue, index int64) {
	return s.rangeKeys(key, end, limit, rangeIndex)
}

func (s *store) TnxPut(key, value []byte) int64 {
	s.put(key, value, s.currentIndex+1)
	return int64(s.currentIndex + 1)
}

func (s *store) TnxDeleteRange(key, end []byte) (n, index int64) {
	n = s.deleteRange(key, end, s.currentIndex+1)
	if n != 0 || s.subIndex != 0 {
		index = int64(s.currentIndex + 1)
	}
	return n, index
}

// range is a keyword in Go, add Keys suffix.
func (s *store) rangeKeys(key, end []byte, limit, rangeIndex int64) (kvs []storagepb.KeyValue, index int64) {
	if rangeIndex <= 0 {
		index = int64(s.currentIndex)
		if s.subIndex > 0 {
			index += 1
		}
	} else {
		index = rangeIndex
	}

	pairs := s.kvindex.Range(key, end, uint64(index))
	if len(pairs) == 0 {
		return nil, index
	}
	if limit > 0 && len(pairs) > int(limit) {
		pairs = pairs[:limit]
	}

	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	for _, pair := range pairs {
		ibytes := make([]byte, 8)
		endbytes := make([]byte, 8)
		binary.BigEndian.PutUint64(ibytes, pair.index)
		binary.BigEndian.PutUint64(endbytes, pair.index+1)

		found := false
		var kv *storagepb.KeyValue

		vs := tx.UnsafeRange(keyBucketName, ibytes, endbytes, 0)
		for _, v := range vs {
			var e storagepb.Event
			err := e.Unmarshal(v)
			if err != nil {
				log.Fatalf("storage: range cannot unmarshal event: %v", err)
			}
			if bytes.Equal(e.Kv.Key, pair.key) {
				if e.Type == storagepb.PUT {
					kv = &e.Kv
				} else {
					kv = nil
				}
				found = true
			}
		}

		if !found {
			log.Fatalf("storage: range cannot find key %s at index %d", string(pair.key), pair.index)
		}
		if kv != nil {
			kvs = append(kvs, *kv)
		}
	}
	return kvs, index
}

func (s *store) put(key, value []byte, index uint64) {
	ibytes := make([]byte, 8+1+4)
	indexToBytes(index, s.subIndex, ibytes)

	event := storagepb.Event{
		Type: storagepb.PUT,
		Kv: storagepb.KeyValue{
			Key:   key,
			Value: value,
		},
	}

	d, err := event.Marshal()
	if err != nil {
		log.Fatalf("storage: cannot marshal event: %v", err)
	}

	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(keyBucketName, ibytes, d)
	s.kvindex.Put(key, index)
	s.subIndex += 1
}

func (s *store) deleteRange(key, end []byte, index uint64) int64 {
	var n int64
	rindex := index
	if s.subIndex > 0 {
		rindex += 1
	}
	pairs := s.kvindex.Range(key, end, rindex)

	if len(pairs) == 0 {
		return 0
	}

	for _, pair := range pairs {
		ok := s.delete(pair.key, index)
		if ok {
			n++
		}
	}
	return n
}

func (s *store) delete(key []byte, index uint64) bool {
	gindex := index
	if s.subIndex > 0 {
		gindex += 1
	}
	_, err := s.kvindex.Get(key, gindex)
	if err != nil {
		// key not exist
		return false
	}

	ibytes := make([]byte, 8+1+4)
	indexToBytes(index, s.subIndex, ibytes)

	event := storagepb.Event{
		Type: storagepb.DELETE,
		Kv: storagepb.KeyValue{
			Key: key,
		},
	}

	d, err := event.Marshal()
	if err != nil {
		log.Fatalf("storage: cannot marshal event: %v", err)
	}

	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(keyBucketName, ibytes, d)
	err = s.kvindex.Tombstone(key, index)
	if err != nil {
		log.Fatalf("storage: cannot tombstone an existing key (%s): %v", string(key), err)
	}
	s.subIndex += 1
	return true
}

func indexToBytes(index uint64, subindex uint32, bytes []byte) {
	binary.BigEndian.PutUint64(bytes, index)
	bytes[8] = '_'
	binary.BigEndian.PutUint32(bytes[9:], subindex)
}
