// Copyright 2018 The etcd Authors
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

package rpcpb

import (
	"reflect"
	"testing"
)

func TestEtcdFlags(t *testing.T) {
	cfg := &Etcd{
		Name:                     "s1",
		DataDir:                  "/tmp/etcd-agent-data-1/etcd.data",
		WALDir:                   "/tmp/etcd-agent-data-1/etcd.data/member/wal",
		ListenClientURLs:         []string{"127.0.0.1:1379"},
		AdvertiseClientURLs:      []string{"127.0.0.1:13790"},
		ListenPeerURLs:           []string{"127.0.0.1:1380"},
		InitialAdvertisePeerURLs: []string{"127.0.0.1:13800"},
		InitialCluster:           "s1=127.0.0.1:13800,s2=127.0.0.1:23800,s3=127.0.0.1:33800",
		InitialClusterState:      "new",
		InitialClusterToken:      "tkn",
		SnapshotCount:            10000,
		QuotaBackendBytes:        10740000000,
		PreVote:                  true,
		InitialCorruptCheck:      true,
	}
	exp := []string{
		"--name=s1",
		"--data-dir=/tmp/etcd-agent-data-1/etcd.data",
		"--wal-dir=/tmp/etcd-agent-data-1/etcd.data/member/wal",
		"--listen-client-urls=127.0.0.1:1379",
		"--advertise-client-urls=127.0.0.1:13790",
		"--listen-peer-urls=127.0.0.1:1380",
		"--initial-advertise-peer-urls=127.0.0.1:13800",
		"--initial-cluster=s1=127.0.0.1:13800,s2=127.0.0.1:23800,s3=127.0.0.1:33800",
		"--initial-cluster-state=new",
		"--initial-cluster-token=tkn",
		"--snapshot-count=10000",
		"--quota-backend-bytes=10740000000",
		"--pre-vote=true",
		"--experimental-initial-corrupt-check=true",
	}
	fs := cfg.Flags()
	if !reflect.DeepEqual(exp, fs) {
		t.Fatalf("expected %q, got %q", exp, fs)
	}
}
