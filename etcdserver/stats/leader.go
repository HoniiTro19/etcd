package stats

import (
	"encoding/json"
	"math"
	"sync"
	"time"
)

// LeaderStats is used by the leader in an etcd cluster, and encapsulates
// statistics about communication with its followers
type LeaderStats struct {
	// TODO(jonboulle): clarify that these are IDs, not names
	Leader    string                    `json:"leader"`
	Followers map[string]*FollowerStats `json:"followers"`

	sync.Mutex
}

// NewLeaderStats generates a new LeaderStats with the given id as leader
func NewLeaderStats(id string) *LeaderStats {
	return &LeaderStats{
		Leader:    id,
		Followers: make(map[string]*FollowerStats),
	}
}

func (ls *LeaderStats) JSON() []byte {
	b, _ := json.Marshal(ls)
	return b
}

func (ls *LeaderStats) Follower(name string) *FollowerStats {
	ls.Lock()
	defer ls.Unlock()
	fs, ok := ls.Followers[name]
	if !ok {
		fs = &FollowerStats{}
		fs.Latency.Minimum = 1 << 63
		ls.Followers[name] = fs
	}
	return fs
}

// FollowerStats encapsulates various statistics about a follower in an etcd cluster
type FollowerStats struct {
	Latency struct {
		Current           float64 `json:"current"`
		Average           float64 `json:"average"`
		averageSquare     float64
		StandardDeviation float64 `json:"standardDeviation"`
		Minimum           float64 `json:"minimum"`
		Maximum           float64 `json:"maximum"`
	} `json:"latency"`

	Counts struct {
		Fail    uint64 `json:"fail"`
		Success uint64 `json:"success"`
	} `json:"counts"`

	sync.Mutex
}

// Succ updates the FollowerStats with a successful send
func (fs *FollowerStats) Succ(d time.Duration) {
	fs.Lock()
	defer fs.Unlock()

	total := float64(fs.Counts.Success) * fs.Latency.Average
	totalSquare := float64(fs.Counts.Success) * fs.Latency.averageSquare

	fs.Counts.Success++

	fs.Latency.Current = float64(d) / (1000000.0)

	if fs.Latency.Current > fs.Latency.Maximum {
		fs.Latency.Maximum = fs.Latency.Current
	}

	if fs.Latency.Current < fs.Latency.Minimum {
		fs.Latency.Minimum = fs.Latency.Current
	}

	fs.Latency.Average = (total + fs.Latency.Current) / float64(fs.Counts.Success)
	fs.Latency.averageSquare = (totalSquare + fs.Latency.Current*fs.Latency.Current) / float64(fs.Counts.Success)

	// sdv = sqrt(avg(x^2) - avg(x)^2)
	fs.Latency.StandardDeviation = math.Sqrt(fs.Latency.averageSquare - fs.Latency.Average*fs.Latency.Average)
}

// Fail updates the FollowerStats with an unsuccessful send
func (fs *FollowerStats) Fail() {
	fs.Lock()
	defer fs.Unlock()
	fs.Counts.Fail++
}
