package raft

import (
	"testing"

	"github.com/coreos/etcd/Godeps/_workspace/src/code.google.com/p/go.net/context"
)

func BenchmarkOneNode(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := newNode()
	r := newRaft(1, []uint64{1}, 10, 1)
	go n.run(r)

	defer n.Stop()

	n.Campaign(ctx)
	for i := 0; i < b.N; i++ {
		<-n.Ready()
		n.Advance()
		n.Propose(ctx, []byte("foo"))
	}
	rd := <-n.Ready()
	if rd.HardState.Commit != uint64(b.N+1) {
		b.Errorf("commit = %d, want %d", rd.HardState.Commit, b.N+1)
	}
}
