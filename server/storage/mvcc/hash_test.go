// Copyright 2022 The etcd Authors
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

package mvcc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.uber.org/zap/zaptest"
)

const (
	// Use high prime
	compactionCycle = 71
)

// Test HashByRevValue values to ensure we don't change the output which would
// have catastrophic consequences. Expected output is just hardcoded, so please
// regenerate it every time you change input parameters.
func TestHashByRevValue(t *testing.T) {
	b, _ := betesting.NewDefaultTmpBackend(t)
	s := NewStore(zaptest.NewLogger(t), b, &lease.FakeLessor{}, StoreConfig{})

	var totalRevisions int64 = 1210
	assert.Less(t, int64(s.cfg.CompactionBatchLimit), totalRevisions)
	assert.Less(t, int64(compactionCycle*10), totalRevisions)
	var rev int64
	var got []KeyValueHash
	for ; rev < totalRevisions; rev += compactionCycle {
		putKVs(s, rev, compactionCycle)
		hash := testHashByRev(t, s, rev+compactionCycle/2)
		got = append(got, hash)
	}
	putKVs(s, rev, totalRevisions)
	hash := testHashByRev(t, s, rev+totalRevisions/2)
	got = append(got, hash)
	assert.Equal(t, []KeyValueHash{
		{4082599214, -1, 35},
		{2279933401, 35, 106},
		{3284231217, 106, 177},
		{126286495, 177, 248},
		{900108730, 248, 319},
		{2475485232, 319, 390},
		{1226296507, 390, 461},
		{2503661030, 461, 532},
		{4155130747, 532, 603},
		{106915399, 603, 674},
		{406914006, 674, 745},
		{1882211381, 745, 816},
		{806177088, 816, 887},
		{664311366, 887, 958},
		{1496914449, 958, 1029},
		{2434525091, 1029, 1100},
		{3988652253, 1100, 1171},
		{1122462288, 1171, 1242},
		{724436716, 1242, 1883},
	}, got)
}

func TestHashByRevValueLastRevision(t *testing.T) {
	b, _ := betesting.NewDefaultTmpBackend(t)
	s := NewStore(zaptest.NewLogger(t), b, &lease.FakeLessor{}, StoreConfig{})

	var totalRevisions int64 = 1210
	assert.Less(t, int64(s.cfg.CompactionBatchLimit), totalRevisions)
	assert.Less(t, int64(compactionCycle*10), totalRevisions)
	var rev int64
	var got []KeyValueHash
	for ; rev < totalRevisions; rev += compactionCycle {
		putKVs(s, rev, compactionCycle)
		hash := testHashByRev(t, s, 0)
		got = append(got, hash)
	}
	putKVs(s, rev, totalRevisions)
	hash := testHashByRev(t, s, 0)
	got = append(got, hash)
	assert.Equal(t, []KeyValueHash{
		{1913897190, -1, 73},
		{224860069, 73, 145},
		{1565167519, 145, 217},
		{1566261620, 217, 289},
		{2037173024, 289, 361},
		{691659396, 361, 433},
		{2713730748, 433, 505},
		{3919322507, 505, 577},
		{769967540, 577, 649},
		{2909194793, 649, 721},
		{1576921157, 721, 793},
		{4067701532, 793, 865},
		{2226384237, 865, 937},
		{2923408134, 937, 1009},
		{2680329256, 1009, 1081},
		{1546717673, 1081, 1153},
		{2713657846, 1153, 1225},
		{1046575299, 1225, 1297},
		{2017735779, 1297, 2508},
	}, got)
}

func putKVs(s *store, rev, count int64) {
	for i := rev; i <= rev+count; i++ {
		s.Put([]byte(pickKey(i)), []byte(fmt.Sprint(i)), 0)
	}
}

func testHashByRev(t *testing.T, s *store, rev int64) KeyValueHash {
	if rev == 0 {
		rev = s.Rev()
	}
	hash, _, err := s.hashByRev(rev)
	assert.NoError(t, err, "error on rev %v", rev)
	_, err = s.Compact(traceutil.TODO(), rev)
	assert.NoError(t, err, "error on compact %v", rev)
	return hash
}

// TODO: Change this to fuzz test
func TestCompactionHash(t *testing.T) {
	b, _ := betesting.NewDefaultTmpBackend(t)
	s := NewStore(zaptest.NewLogger(t), b, &lease.FakeLessor{}, StoreConfig{})

	var totalRevisions int64 = 1210
	assert.Less(t, int64(s.cfg.CompactionBatchLimit), totalRevisions)
	assert.Less(t, int64(compactionCycle*10), totalRevisions)
	var rev int64
	for ; rev < totalRevisions; rev += compactionCycle {
		testCompactionHash(t, s, rev, rev+compactionCycle)
	}
	testCompactionHash(t, s, rev, rev+totalRevisions)
}

func testCompactionHash(t *testing.T, s *store, start, stop int64) {
	for i := start; i <= stop; i++ {
		s.Put([]byte(pickKey(i)), []byte(fmt.Sprint(i)), 0)
	}
	hash1, _, err := s.hashByRev(stop)
	assert.NoError(t, err, "error on rev %v", stop)

	_, prevCompactRev, err := s.updateCompactRev(stop)
	assert.NoError(t, err, "error on rev %v", stop)

	hash2, err := s.scheduleCompaction(stop, prevCompactRev)
	assert.NoError(t, err, "error on rev %v", stop)
	assert.Equal(t, hash1, hash2, "hashes do not match on rev %v", stop)
}

func pickKey(i int64) string {
	if i%(compactionCycle*2) == 30 {
		return "zenek"
	}
	if i%compactionCycle == 30 {
		return "xavery"
	}
	// Use low prime number to ensure repeats without alignment
	switch i % 7 {
	case 0:
		return "alice"
	case 1:
		return "bob"
	case 2:
		return "celine"
	case 3:
		return "dominik"
	case 4:
		return "eve"
	case 5:
		return "frederica"
	case 6:
		return "gorge"
	default:
		panic("Can't count")
	}
}

func TestHasherStore(t *testing.T) {
	lg := zaptest.NewLogger(t)
	s := newHashStorage(lg, newFakeStore(lg))
	var hashes []KeyValueHash
	for i := 0; i < hashStorageMaxSize; i++ {
		hash := KeyValueHash{Hash: uint32(i), Revision: int64(i) + 10, CompactRevision: int64(i) + 100}
		hashes = append(hashes, hash)
		s.Store(hash)
	}

	for _, want := range hashes {
		got, _, err := s.HashByRev(want.Revision)
		if err != nil {
			t.Fatal(err)
		}
		if want.Hash != got.Hash {
			t.Errorf("Expected stored hash to match, got: %d, expected: %d", want.Hash, got.Hash)
		}
		if want.Revision != got.Revision {
			t.Errorf("Expected stored revision to match, got: %d, expected: %d", want.Revision, got.Revision)
		}
		if want.CompactRevision != got.CompactRevision {
			t.Errorf("Expected stored compact revision to match, got: %d, expected: %d", want.CompactRevision, got.CompactRevision)
		}
	}
}

func TestHasherStoreFull(t *testing.T) {
	lg := zaptest.NewLogger(t)
	s := newHashStorage(lg, newFakeStore(lg))
	var minRevision int64 = 100
	var maxRevision = minRevision + hashStorageMaxSize
	for i := 0; i < hashStorageMaxSize; i++ {
		s.Store(KeyValueHash{Revision: int64(i) + minRevision})
	}

	// Hash for old revision should be discarded as storage is already full
	s.Store(KeyValueHash{Revision: minRevision - 1})
	hash, _, err := s.HashByRev(minRevision - 1)
	if err == nil {
		t.Errorf("Expected an error as old revision should be discarded, got: %v", hash)
	}
	// Hash for new revision should be stored even when storage is full
	s.Store(KeyValueHash{Revision: maxRevision + 1})
	_, _, err = s.HashByRev(maxRevision + 1)
	if err != nil {
		t.Errorf("Didn't expect error for new revision, err: %v", err)
	}
}
