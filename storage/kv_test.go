// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/pkg/testutil"
	"github.com/coreos/etcd/storage/backend"
	"github.com/coreos/etcd/storage/storagepb"
)

// Functional tests for features implemented in v3 store. It treats v3 store
// as a black box, and tests it by feeding the input and validating the output.

// TODO: add similar tests on operations in one txn/rev

type (
	rangeFunc       func(kv KV, key, end []byte, limit, rangeRev int64) ([]storagepb.KeyValue, int64, error)
	putFunc         func(kv KV, key, value []byte, lease lease.LeaseID) int64
	deleteRangeFunc func(kv KV, key, end []byte) (n, rev int64)
)

var (
	normalRangeFunc = func(kv KV, key, end []byte, limit, rangeRev int64) ([]storagepb.KeyValue, int64, error) {
		return kv.Range(key, end, limit, rangeRev)
	}
	txnRangeFunc = func(kv KV, key, end []byte, limit, rangeRev int64) ([]storagepb.KeyValue, int64, error) {
		id := kv.TxnBegin()
		defer kv.TxnEnd(id)
		return kv.TxnRange(id, key, end, limit, rangeRev)
	}

	normalPutFunc = func(kv KV, key, value []byte, lease lease.LeaseID) int64 {
		return kv.Put(key, value, lease)
	}
	txnPutFunc = func(kv KV, key, value []byte, lease lease.LeaseID) int64 {
		id := kv.TxnBegin()
		defer kv.TxnEnd(id)
		rev, err := kv.TxnPut(id, key, value, lease)
		if err != nil {
			panic("txn put error")
		}
		return rev
	}

	normalDeleteRangeFunc = func(kv KV, key, end []byte) (n, rev int64) {
		return kv.DeleteRange(key, end)
	}
	txnDeleteRangeFunc = func(kv KV, key, end []byte) (n, rev int64) {
		id := kv.TxnBegin()
		defer kv.TxnEnd(id)
		n, rev, err := kv.TxnDeleteRange(id, key, end)
		if err != nil {
			panic("txn delete error")
		}
		return n, rev
	}
)

func TestKVRange(t *testing.T)    { testKVRange(t, normalRangeFunc) }
func TestKVTxnRange(t *testing.T) { testKVRange(t, txnRangeFunc) }

func testKVRange(t *testing.T, f rangeFunc) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar"), 1)
	s.Put([]byte("foo1"), []byte("bar1"), 2)
	s.Put([]byte("foo2"), []byte("bar2"), 3)
	kvs := []storagepb.KeyValue{
		{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 1},
		{Key: []byte("foo1"), Value: []byte("bar1"), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: 2},
		{Key: []byte("foo2"), Value: []byte("bar2"), CreateRevision: 3, ModRevision: 3, Version: 1, Lease: 3},
	}

	wrev := int64(3)
	tests := []struct {
		key, end []byte
		wkvs     []storagepb.KeyValue
	}{
		// get no keys
		{
			[]byte("doo"), []byte("foo"),
			nil,
		},
		// get no keys when key == end
		{
			[]byte("foo"), []byte("foo"),
			nil,
		},
		// get no keys when ranging single key
		{
			[]byte("doo"), nil,
			nil,
		},
		// get all keys
		{
			[]byte("foo"), []byte("foo3"),
			kvs,
		},
		// get partial keys
		{
			[]byte("foo"), []byte("foo1"),
			kvs[:1],
		},
		// get single key
		{
			[]byte("foo"), nil,
			kvs[:1],
		},
	}

	for i, tt := range tests {
		kvs, rev, err := f(s, tt.key, tt.end, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if rev != wrev {
			t.Errorf("#%d: rev = %d, want %d", i, rev, wrev)
		}
		if !reflect.DeepEqual(kvs, tt.wkvs) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, tt.wkvs)
		}
	}
}

func TestKVRangeRev(t *testing.T)    { testKVRangeRev(t, normalRangeFunc) }
func TestKVTxnRangeRev(t *testing.T) { testKVRangeRev(t, normalRangeFunc) }

func testKVRangeRev(t *testing.T, f rangeFunc) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar"), 1)
	s.Put([]byte("foo1"), []byte("bar1"), 2)
	s.Put([]byte("foo2"), []byte("bar2"), 3)
	kvs := []storagepb.KeyValue{
		{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 1},
		{Key: []byte("foo1"), Value: []byte("bar1"), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: 2},
		{Key: []byte("foo2"), Value: []byte("bar2"), CreateRevision: 3, ModRevision: 3, Version: 1, Lease: 3},
	}

	tests := []struct {
		rev  int64
		wrev int64
		wkvs []storagepb.KeyValue
	}{
		{-1, 3, kvs},
		{0, 3, kvs},
		{1, 1, kvs[:1]},
		{2, 2, kvs[:2]},
		{3, 3, kvs},
	}

	for i, tt := range tests {
		kvs, rev, err := f(s, []byte("foo"), []byte("foo3"), 0, tt.rev)
		if err != nil {
			t.Fatal(err)
		}
		if rev != tt.wrev {
			t.Errorf("#%d: rev = %d, want %d", i, rev, tt.wrev)
		}
		if !reflect.DeepEqual(kvs, tt.wkvs) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, tt.wkvs)
		}
	}
}

func TestKVRangeBadRev(t *testing.T)    { testKVRangeBadRev(t, normalRangeFunc) }
func TestKVTxnRangeBadRev(t *testing.T) { testKVRangeBadRev(t, normalRangeFunc) }

func testKVRangeBadRev(t *testing.T, f rangeFunc) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar"), NoLease)
	s.Put([]byte("foo1"), []byte("bar1"), NoLease)
	s.Put([]byte("foo2"), []byte("bar2"), NoLease)
	if err := s.Compact(3); err != nil {
		t.Fatalf("compact error (%v)", err)
	}

	tests := []struct {
		rev  int64
		werr error
	}{
		{-1, ErrCompacted},
		{2, ErrCompacted},
		{3, ErrCompacted},
		{4, ErrFutureRev},
		{100, ErrFutureRev},
	}
	for i, tt := range tests {
		_, _, err := f(s, []byte("foo"), []byte("foo3"), 0, tt.rev)
		if err != tt.werr {
			t.Errorf("#%d: error = %v, want %v", i, err, tt.werr)
		}
	}
}

func TestKVRangeLimit(t *testing.T)    { testKVRangeLimit(t, normalRangeFunc) }
func TestKVTxnRangeLimit(t *testing.T) { testKVRangeLimit(t, txnRangeFunc) }

func testKVRangeLimit(t *testing.T, f rangeFunc) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar"), 1)
	s.Put([]byte("foo1"), []byte("bar1"), 2)
	s.Put([]byte("foo2"), []byte("bar2"), 3)
	kvs := []storagepb.KeyValue{
		{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 1},
		{Key: []byte("foo1"), Value: []byte("bar1"), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: 2},
		{Key: []byte("foo2"), Value: []byte("bar2"), CreateRevision: 3, ModRevision: 3, Version: 1, Lease: 3},
	}

	wrev := int64(3)
	tests := []struct {
		limit int64
		wkvs  []storagepb.KeyValue
	}{
		// no limit
		{-1, kvs},
		// no limit
		{0, kvs},
		{1, kvs[:1]},
		{2, kvs[:2]},
		{3, kvs},
		{100, kvs},
	}
	for i, tt := range tests {
		kvs, rev, err := f(s, []byte("foo"), []byte("foo3"), tt.limit, 0)
		if err != nil {
			t.Fatalf("#%d: range error (%v)", i, err)
		}
		if !reflect.DeepEqual(kvs, tt.wkvs) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, tt.wkvs)
		}
		if rev != wrev {
			t.Errorf("#%d: rev = %d, want %d", i, rev, wrev)
		}
	}
}

func TestKVPutMultipleTimes(t *testing.T)    { testKVPutMultipleTimes(t, normalPutFunc) }
func TestKVTxnPutMultipleTimes(t *testing.T) { testKVPutMultipleTimes(t, txnPutFunc) }

func testKVPutMultipleTimes(t *testing.T, f putFunc) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	for i := 0; i < 10; i++ {
		base := int64(i + 1)

		rev := f(s, []byte("foo"), []byte("bar"), lease.LeaseID(base))
		if rev != base {
			t.Errorf("#%d: rev = %d, want %d", i, rev, base)
		}

		kvs, _, err := s.Range([]byte("foo"), nil, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		wkvs := []storagepb.KeyValue{
			{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 1, ModRevision: base, Version: base, Lease: base},
		}
		if !reflect.DeepEqual(kvs, wkvs) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, wkvs)
		}
	}
}

func TestKVDeleteRange(t *testing.T)    { testKVDeleteRange(t, normalDeleteRangeFunc) }
func TestKVTxnDeleteRange(t *testing.T) { testKVDeleteRange(t, txnDeleteRangeFunc) }

func testKVDeleteRange(t *testing.T, f deleteRangeFunc) {
	tests := []struct {
		key, end []byte

		wrev int64
		wN   int64
	}{
		{
			[]byte("foo"), nil,
			4, 1,
		},
		{
			[]byte("foo"), []byte("foo1"),
			4, 1,
		},
		{
			[]byte("foo"), []byte("foo2"),
			4, 2,
		},
		{
			[]byte("foo"), []byte("foo3"),
			4, 3,
		},
		{
			[]byte("foo3"), []byte("foo8"),
			3, 0,
		},
		{
			[]byte("foo3"), nil,
			3, 0,
		},
	}

	for i, tt := range tests {
		b, tmpPath := backend.NewDefaultTmpBackend()
		s := NewStore(b)

		s.Put([]byte("foo"), []byte("bar"), NoLease)
		s.Put([]byte("foo1"), []byte("bar1"), NoLease)
		s.Put([]byte("foo2"), []byte("bar2"), NoLease)

		n, rev := f(s, tt.key, tt.end)
		if n != tt.wN || rev != tt.wrev {
			t.Errorf("#%d: n = %d, rev = %d, want (%d, %d)", i, n, rev, tt.wN, tt.wrev)
		}

		cleanup(s, b, tmpPath)
	}
}

func TestKVDeleteMultipleTimes(t *testing.T)    { testKVDeleteMultipleTimes(t, normalDeleteRangeFunc) }
func TestKVTxnDeleteMultipleTimes(t *testing.T) { testKVDeleteMultipleTimes(t, txnDeleteRangeFunc) }

func testKVDeleteMultipleTimes(t *testing.T, f deleteRangeFunc) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar"), NoLease)

	n, rev := f(s, []byte("foo"), nil)
	if n != 1 || rev != 2 {
		t.Fatalf("n = %d, rev = %d, want (%d, %d)", n, rev, 1, 2)
	}

	for i := 0; i < 10; i++ {
		n, rev := f(s, []byte("foo"), nil)
		if n != 0 || rev != 2 {
			t.Fatalf("#%d: n = %d, rev = %d, want (%d, %d)", i, n, rev, 0, 2)
		}
	}
}

// test that range, put, delete on single key in sequence repeatedly works correctly.
func TestKVOperationInSequence(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	for i := 0; i < 10; i++ {
		base := int64(i * 2)

		// put foo
		rev := s.Put([]byte("foo"), []byte("bar"), NoLease)
		if rev != base+1 {
			t.Errorf("#%d: put rev = %d, want %d", i, rev, base+1)
		}

		kvs, rev, err := s.Range([]byte("foo"), nil, 0, base+1)
		if err != nil {
			t.Fatal(err)
		}
		wkvs := []storagepb.KeyValue{
			{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: base + 1, ModRevision: base + 1, Version: 1, Lease: int64(NoLease)},
		}
		if !reflect.DeepEqual(kvs, wkvs) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, wkvs)
		}
		if rev != base+1 {
			t.Errorf("#%d: range rev = %d, want %d", i, rev, base+1)
		}

		// delete foo
		n, rev := s.DeleteRange([]byte("foo"), nil)
		if n != 1 || rev != base+2 {
			t.Errorf("#%d: n = %d, rev = %d, want (%d, %d)", i, n, rev, 1, base+2)
		}

		kvs, rev, err = s.Range([]byte("foo"), nil, 0, base+2)
		if err != nil {
			t.Fatal(err)
		}
		if kvs != nil {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, nil)
		}
		if rev != base+2 {
			t.Errorf("#%d: range rev = %d, want %d", i, rev, base+2)
		}
	}
}

func TestKVTxnBlockNonTnxOperations(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	tests := []func(){
		func() { s.Range([]byte("foo"), nil, 0, 0) },
		func() { s.Put([]byte("foo"), nil, NoLease) },
		func() { s.DeleteRange([]byte("foo"), nil) },
	}
	for i, tt := range tests {
		id := s.TxnBegin()
		done := make(chan struct{})
		go func() {
			tt()
			done <- struct{}{}
		}()
		select {
		case <-done:
			t.Fatalf("#%d: operation failed to be blocked", i)
		case <-time.After(10 * time.Millisecond):
		}

		s.TxnEnd(id)
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("#%d: operation failed to be unblocked", i)
		}
	}
}

func TestKVTxnWrongID(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	id := s.TxnBegin()
	wrongid := id + 1

	tests := []func() error{
		func() error {
			_, _, err := s.TxnRange(wrongid, []byte("foo"), nil, 0, 0)
			return err
		},
		func() error {
			_, err := s.TxnPut(wrongid, []byte("foo"), nil, NoLease)
			return err
		},
		func() error {
			_, _, err := s.TxnDeleteRange(wrongid, []byte("foo"), nil)
			return err
		},
		func() error { return s.TxnEnd(wrongid) },
	}
	for i, tt := range tests {
		err := tt()
		if err != ErrTxnIDMismatch {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, ErrTxnIDMismatch)
		}
	}

	err := s.TxnEnd(id)
	if err != nil {
		t.Fatalf("end err = %+v, want %+v", err, nil)
	}
}

// test that txn range, put, delete on single key in sequence repeatedly works correctly.
func TestKVTnxOperationInSequence(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	for i := 0; i < 10; i++ {
		id := s.TxnBegin()
		base := int64(i)

		// put foo
		rev, err := s.TxnPut(id, []byte("foo"), []byte("bar"), NoLease)
		if err != nil {
			t.Fatal(err)
		}
		if rev != base+1 {
			t.Errorf("#%d: put rev = %d, want %d", i, rev, base+1)
		}

		kvs, rev, err := s.TxnRange(id, []byte("foo"), nil, 0, base+1)
		if err != nil {
			t.Fatal(err)
		}
		wkvs := []storagepb.KeyValue{
			{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: base + 1, ModRevision: base + 1, Version: 1, Lease: int64(NoLease)},
		}
		if !reflect.DeepEqual(kvs, wkvs) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, wkvs)
		}
		if rev != base+1 {
			t.Errorf("#%d: range rev = %d, want %d", i, rev, base+1)
		}

		// delete foo
		n, rev, err := s.TxnDeleteRange(id, []byte("foo"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 || rev != base+1 {
			t.Errorf("#%d: n = %d, rev = %d, want (%d, %d)", i, n, rev, 1, base+1)
		}

		kvs, rev, err = s.TxnRange(id, []byte("foo"), nil, 0, base+1)
		if err != nil {
			t.Errorf("#%d: range error (%v)", i, err)
		}
		if kvs != nil {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, nil)
		}
		if rev != base+1 {
			t.Errorf("#%d: range rev = %d, want %d", i, rev, base+1)
		}

		s.TxnEnd(id)
	}
}

func TestKVCompactReserveLastValue(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar0"), 1)
	s.Put([]byte("foo"), []byte("bar1"), 2)
	s.DeleteRange([]byte("foo"), nil)
	s.Put([]byte("foo"), []byte("bar2"), 3)

	// rev in tests will be called in Compact() one by one on the same store
	tests := []struct {
		rev int64
		// wanted kvs right after the compacted rev
		wkvs []storagepb.KeyValue
	}{
		{
			0,
			[]storagepb.KeyValue{
				{Key: []byte("foo"), Value: []byte("bar0"), CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 1},
			},
		},
		{
			1,
			[]storagepb.KeyValue{
				{Key: []byte("foo"), Value: []byte("bar1"), CreateRevision: 1, ModRevision: 2, Version: 2, Lease: 2},
			},
		},
		{
			2,
			nil,
		},
		{
			3,
			[]storagepb.KeyValue{
				{Key: []byte("foo"), Value: []byte("bar2"), CreateRevision: 4, ModRevision: 4, Version: 1, Lease: 3},
			},
		},
	}
	for i, tt := range tests {
		err := s.Compact(tt.rev)
		if err != nil {
			t.Errorf("#%d: unexpect compact error %v", i, err)
		}
		kvs, _, err := s.Range([]byte("foo"), nil, 0, tt.rev+1)
		if err != nil {
			t.Errorf("#%d: unexpect range error %v", i, err)
		}
		if !reflect.DeepEqual(kvs, tt.wkvs) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, kvs, tt.wkvs)
		}
	}
}

func TestKVCompactBad(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar0"), NoLease)
	s.Put([]byte("foo"), []byte("bar1"), NoLease)
	s.Put([]byte("foo"), []byte("bar2"), NoLease)

	// rev in tests will be called in Compact() one by one on the same store
	tests := []struct {
		rev  int64
		werr error
	}{
		{0, nil},
		{1, nil},
		{1, ErrCompacted},
		{3, nil},
		{4, ErrFutureRev},
		{100, ErrFutureRev},
	}
	for i, tt := range tests {
		err := s.Compact(tt.rev)
		if err != tt.werr {
			t.Errorf("#%d: compact error = %v, want %v", i, err, tt.werr)
		}
	}
}

func TestKVHash(t *testing.T) {
	hashes := make([]uint32, 3)

	for i := 0; i < len(hashes); i++ {
		var err error
		b, tmpPath := backend.NewDefaultTmpBackend()
		kv := NewStore(b)
		kv.Put([]byte("foo0"), []byte("bar0"), NoLease)
		kv.Put([]byte("foo1"), []byte("bar0"), NoLease)
		hashes[i], err = kv.Hash()
		if err != nil {
			t.Fatalf("failed to get hash: %v", err)
		}
		cleanup(kv, b, tmpPath)
	}

	for i := 1; i < len(hashes); i++ {
		if hashes[i-1] != hashes[i] {
			t.Errorf("hash[%d](%d) != hash[%d](%d)", i-1, hashes[i-1], i, hashes[i])
		}
	}
}

func TestKVRestore(t *testing.T) {
	tests := []func(kv KV){
		func(kv KV) {
			kv.Put([]byte("foo"), []byte("bar0"), 1)
			kv.Put([]byte("foo"), []byte("bar1"), 2)
			kv.Put([]byte("foo"), []byte("bar2"), 3)
		},
		func(kv KV) {
			kv.Put([]byte("foo"), []byte("bar0"), 1)
			kv.DeleteRange([]byte("foo"), nil)
			kv.Put([]byte("foo"), []byte("bar1"), 2)
		},
		func(kv KV) {
			kv.Put([]byte("foo"), []byte("bar0"), 1)
			kv.Put([]byte("foo"), []byte("bar1"), 2)
			kv.Compact(1)
		},
	}
	for i, tt := range tests {
		b, tmpPath := backend.NewDefaultTmpBackend()
		s := NewStore(b)
		tt(s)
		var kvss [][]storagepb.KeyValue
		for k := int64(0); k < 10; k++ {
			kvs, _, _ := s.Range([]byte("a"), []byte("z"), 0, k)
			kvss = append(kvss, kvs)
		}
		s.Close()

		// ns should recover the the previous state from backend.
		ns := NewStore(b)
		// wait for possible compaction to finish
		testutil.WaitSchedule()
		var nkvss [][]storagepb.KeyValue
		for k := int64(0); k < 10; k++ {
			nkvs, _, _ := ns.Range([]byte("a"), []byte("z"), 0, k)
			nkvss = append(nkvss, nkvs)
		}
		cleanup(ns, b, tmpPath)

		if !reflect.DeepEqual(nkvss, kvss) {
			t.Errorf("#%d: kvs history = %+v, want %+v", i, nkvss, kvss)
		}
	}
}

func TestKVSnapshot(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := NewStore(b)
	defer cleanup(s, b, tmpPath)

	s.Put([]byte("foo"), []byte("bar"), 1)
	s.Put([]byte("foo1"), []byte("bar1"), 2)
	s.Put([]byte("foo2"), []byte("bar2"), 3)
	wkvs := []storagepb.KeyValue{
		{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 1},
		{Key: []byte("foo1"), Value: []byte("bar1"), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: 2},
		{Key: []byte("foo2"), Value: []byte("bar2"), CreateRevision: 3, ModRevision: 3, Version: 1, Lease: 3},
	}

	newPath := "new_test"
	f, err := os.Create(newPath)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(newPath)

	snap := s.Snapshot()
	defer snap.Close()
	_, err = snap.WriteTo(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	ns := NewStore(b)
	defer ns.Close()
	kvs, rev, err := ns.Range([]byte("a"), []byte("z"), 0, 0)
	if err != nil {
		t.Errorf("unexpect range error (%v)", err)
	}
	if !reflect.DeepEqual(kvs, wkvs) {
		t.Errorf("kvs = %+v, want %+v", kvs, wkvs)
	}
	if rev != 3 {
		t.Errorf("rev = %d, want %d", rev, 3)
	}
}

func TestWatchableKVWatch(t *testing.T) {
	b, tmpPath := backend.NewDefaultTmpBackend()
	s := WatchableKV(newWatchableStore(b))
	defer cleanup(s, b, tmpPath)

	w := s.NewWatchStream()
	defer w.Close()

	wid := w.Watch([]byte("foo"), true, 0)

	s.Put([]byte("foo"), []byte("bar"), 1)
	select {
	case resp := <-w.Chan():
		wev := storagepb.Event{
			Type: storagepb.PUT,
			Kv: &storagepb.KeyValue{
				Key:            []byte("foo"),
				Value:          []byte("bar"),
				CreateRevision: 1,
				ModRevision:    1,
				Version:        1,
				Lease:          1,
			},
		}
		if resp.WatchID != wid {
			t.Errorf("resp.WatchID got = %d, want = %d", resp.WatchID, wid)
		}
		ev := resp.Events[0]
		if !reflect.DeepEqual(ev, wev) {
			t.Errorf("watched event = %+v, want %+v", ev, wev)
		}
	case <-time.After(time.Second):
		t.Fatalf("failed to watch the event")
	}

	s.Put([]byte("foo1"), []byte("bar1"), 2)
	select {
	case resp := <-w.Chan():
		wev := storagepb.Event{
			Type: storagepb.PUT,
			Kv: &storagepb.KeyValue{
				Key:            []byte("foo1"),
				Value:          []byte("bar1"),
				CreateRevision: 2,
				ModRevision:    2,
				Version:        1,
				Lease:          2,
			},
		}
		if resp.WatchID != wid {
			t.Errorf("resp.WatchID got = %d, want = %d", resp.WatchID, wid)
		}
		ev := resp.Events[0]
		if !reflect.DeepEqual(ev, wev) {
			t.Errorf("watched event = %+v, want %+v", ev, wev)
		}
	case <-time.After(time.Second):
		t.Fatalf("failed to watch the event")
	}

	w = s.NewWatchStream()
	wid = w.Watch([]byte("foo1"), false, 1)

	select {
	case resp := <-w.Chan():
		wev := storagepb.Event{
			Type: storagepb.PUT,
			Kv: &storagepb.KeyValue{
				Key:            []byte("foo1"),
				Value:          []byte("bar1"),
				CreateRevision: 2,
				ModRevision:    2,
				Version:        1,
				Lease:          2,
			},
		}
		if resp.WatchID != wid {
			t.Errorf("resp.WatchID got = %d, want = %d", resp.WatchID, wid)
		}
		ev := resp.Events[0]
		if !reflect.DeepEqual(ev, wev) {
			t.Errorf("watched event = %+v, want %+v", ev, wev)
		}
	case <-time.After(time.Second):
		t.Fatalf("failed to watch the event")
	}

	s.Put([]byte("foo1"), []byte("bar11"), 3)
	select {
	case resp := <-w.Chan():
		wev := storagepb.Event{
			Type: storagepb.PUT,
			Kv: &storagepb.KeyValue{
				Key:            []byte("foo1"),
				Value:          []byte("bar11"),
				CreateRevision: 2,
				ModRevision:    3,
				Version:        2,
				Lease:          3,
			},
		}
		if resp.WatchID != wid {
			t.Errorf("resp.WatchID got = %d, want = %d", resp.WatchID, wid)
		}
		ev := resp.Events[0]
		if !reflect.DeepEqual(ev, wev) {
			t.Errorf("watched event = %+v, want %+v", ev, wev)
		}
	case <-time.After(time.Second):
		t.Fatalf("failed to watch the event")
	}
}

func cleanup(s KV, b backend.Backend, path string) {
	s.Close()
	b.Close()
	os.Remove(path)
}
