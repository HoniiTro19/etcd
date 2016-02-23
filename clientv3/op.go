// Copyright 2016 CoreOS, Inc.
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

package clientv3

import (
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/lease"
)

type opType int

const (
	// A default Op has opType 0, which is invalid.
	tRange opType = iota + 1
	tPut
	tDeleteRange
)

// Op represents an Operation that kv can execute.
type Op struct {
	t   opType
	key []byte
	end []byte

	// for range
	limit        int64
	rev          int64
	sort         *SortOption
	serializable bool

	// for put
	val     []byte
	leaseID lease.LeaseID
}

func (op Op) toRequestUnion() *pb.RequestUnion {
	switch op.t {
	case tRange:
		r := &pb.RangeRequest{Key: op.key, RangeEnd: op.end, Limit: op.limit, Revision: op.rev}
		if op.sort != nil {
			r.SortOrder = pb.RangeRequest_SortOrder(op.sort.Order)
			r.SortTarget = pb.RangeRequest_SortTarget(op.sort.Target)
		}
		return &pb.RequestUnion{Request: &pb.RequestUnion_RequestRange{RequestRange: r}}
	case tPut:
		r := &pb.PutRequest{Key: op.key, Value: op.val, Lease: int64(op.leaseID)}
		return &pb.RequestUnion{Request: &pb.RequestUnion_RequestPut{RequestPut: r}}
	case tDeleteRange:
		r := &pb.DeleteRangeRequest{Key: op.key, RangeEnd: op.end}
		return &pb.RequestUnion{Request: &pb.RequestUnion_RequestDeleteRange{RequestDeleteRange: r}}
	default:
		panic("Unknown Op")
	}
}

func (op Op) isWrite() bool {
	return op.t != tRange
}

func OpGet(key string, opts ...OpOption) Op {
	ret := Op{t: tRange, key: []byte(key)}
	ret.applyOpts(opts)
	return ret
}

func OpDelete(key string, opts ...OpOption) Op {
	ret := Op{t: tDeleteRange, key: []byte(key)}
	ret.applyOpts(opts)
	switch {
	case ret.leaseID != 0:
		panic("unexpected lease in delete")
	case ret.limit != 0:
		panic("unexpected limit in delete")
	case ret.rev != 0:
		panic("unexpected revision in delete")
	case ret.sort != nil:
		panic("unexpected sort in delete")
	case ret.serializable != false:
		panic("unexpected serializable in delete")
	}
	return ret
}

func OpPut(key, val string, opts ...OpOption) Op {
	ret := Op{t: tPut, key: []byte(key), val: []byte(val)}
	ret.applyOpts(opts)
	switch {
	case ret.end != nil:
		panic("unexpected range in put")
	case ret.limit != 0:
		panic("unexpected limit in put")
	case ret.rev != 0:
		panic("unexpected revision in put")
	case ret.sort != nil:
		panic("unexpected sort in put")
	case ret.serializable != false:
		panic("unexpected serializable in delete")
	}
	return ret
}

func (op *Op) applyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// OpOption configures Operations like Get, Put, Delete.
type OpOption func(*Op)

// WithLease attaches a lease ID to a key in 'Put' request.
func WithLease(leaseID lease.LeaseID) OpOption {
	return func(op *Op) { op.leaseID = leaseID }
}

// WithLimit limits the number of results to return from 'Get' request.
func WithLimit(n int64) OpOption { return func(op *Op) { op.limit = n } }

// WithRev specifies the store revision for 'Get' request.
//
// TODO: support Watch API
func WithRev(rev int64) OpOption { return func(op *Op) { op.rev = rev } }

// WithSort specifies the ordering in 'Get' request. It requires
// 'WithRange' and/or 'WithPrefix' to be specified too.
// 'target' specifies the target to sort by: key, version, revisions, value.
// 'order' can be either 'SortNone', 'SortAscend', 'SortDescend'.
func WithSort(target SortTarget, order SortOrder) OpOption {
	return func(op *Op) {
		op.sort = &SortOption{target, order}
	}
}

// WithPrefix enables 'Get' or 'Delete' requests to operate on the
// keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
//
// TODO: support Watch API
func WithPrefix() OpOption {
	return func(op *Op) {
		op.end = make([]byte, len(op.key))
		copy(op.end, op.key)
		for i := len(op.end) - 1; i >= 0; i-- {
			if op.end[i] < 0xff {
				op.end[i] = op.end[i] + 1
				op.end = op.end[:i+1]
				return
			}
		}
		// next prefix does not exist (e.g., 0xffff);
		// default to WithFromKey policy
		op.end = []byte{0}
	}
}

// WithRange specifies the range of 'Get' or 'Delete' requests.
// For example, 'Get' requests with 'WithRange(end)' returns
// the keys in the range [key, end).
func WithRange(endKey string) OpOption {
	return func(op *Op) { op.end = []byte(endKey) }
}

// WithFromKey specifies the range of 'Get' or 'Delete' requests
// to be equal or greater than they key in the argument.
func WithFromKey() OpOption { return WithRange("\x00") }

// WithSerializable makes 'Get' request serializable. By default,
// it's linearizable. Serializable requests are better for lower latency
// requirement.
func WithSerializable() OpOption {
	return func(op *Op) { op.serializable = true }
}
