// Package raft implements raft.
package raft

import (
	"sort"

	"code.google.com/p/go.net/context"
)

type Ready struct {
	// The current state of a Node
	State

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []Entry

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	Messages []Message
}

func (a State) Equal(b State) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.LastIndex == b.LastIndex
}

func (rd Ready) containsUpdates(prev Ready) bool {
	return !prev.State.Equal(rd.State) || len(rd.Entries) > 0 || len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0
}

type Node struct {
	ctx    context.Context
	propc  chan Message
	recvc  chan Message
	readyc chan Ready
	tickc  chan struct{}
}

func Start(ctx context.Context, id int64, peers []int64) Node {
	n := Node{
		ctx:    ctx,
		propc:  make(chan Message),
		recvc:  make(chan Message),
		readyc: make(chan Ready),
		tickc:  make(chan struct{}),
	}
	r := newRaft(id, peers)
	go n.run(r)
	return n
}

func (n *Node) run(r *raft) {
	propc := n.propc
	readyc := n.readyc

	var prev Ready
	for {
		if r.hasLeader() {
			propc = n.propc
		} else {
			// We cannot accept proposals because we don't know who
			// to send them to, so we'll apply back-pressure and
			// block senders.
			propc = nil
		}

		rd := Ready{
			r.State,
			r.raftLog.unstableEnts(),
			r.raftLog.nextEnts(),
			r.msgs,
		}

		if rd.containsUpdates(prev) {
			readyc = n.readyc
		} else {
			readyc = nil
		}

		select {
		case m := <-propc:
			m.From = r.id
			r.Step(m)
		case m := <-n.recvc:
			r.Step(m) // raft never returns an error
		case <-n.tickc:
			// r.tick()
		case readyc <- rd:
			r.raftLog.resetNextEnts()
			r.raftLog.resetUnstable()
			r.msgs = nil
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) Tick() error {
	select {
	case n.tickc <- struct{}{}:
		return nil
	case <-n.ctx.Done():
		return n.ctx.Err()
	}
}

// Propose proposes data be appended to the log.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.Step(ctx, []Message{{Type: msgProp, Entries: []Entry{{Data: data}}}})
}

// Step advances the state machine using msgs. Proposals are priotized last so
// that any votes and vote requests will not be wedged behind proposals and
// prevent this cluster from making progress. The ctx.Err() will be returned,
// if any.
func (n *Node) Step(ctx context.Context, msgs []Message) error {
	sort.Sort(sort.Reverse(byMsgType(msgs)))
	for _, m := range msgs {
		ch := n.recvc
		if m.Type == msgProp {
			ch = n.propc
		}

		select {
		case ch <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return n.ctx.Err()
		}
	}
	return nil
}

// ReadState returns the current point-in-time state.
func (n *Node) Ready() <-chan Ready {
	return n.readyc
}

type byMsgType []Message

func (msgs byMsgType) Len() int           { return len(msgs) }
func (msgs byMsgType) Less(i, j int) bool { return msgs[i].Type == msgProp }
func (msgs byMsgType) Swap(i, j int)      { msgs[i], msgs[j] = msgs[i], msgs[j] }
