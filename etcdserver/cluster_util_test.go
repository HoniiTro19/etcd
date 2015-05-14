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

package etcdserver

import (
	"reflect"
	"testing"

	"github.com/coreos/etcd/Godeps/_workspace/src/github.com/coreos/go-semver/semver"
	"github.com/coreos/etcd/version"
)

func TestDecideClusterVersion(t *testing.T) {
	tests := []struct {
		vers  map[string]*version.Versions
		wdver *semver.Version
	}{
		{
			map[string]*version.Versions{"a": &version.Versions{Server: "2.0.0"}},
			semver.Must(semver.NewVersion("2.0.0")),
		},
		// unknow
		{
			map[string]*version.Versions{"a": nil},
			nil,
		},
		{
			map[string]*version.Versions{"a": &version.Versions{Server: "2.0.0"}, "b": &version.Versions{Server: "2.1.0"}, "c": &version.Versions{Server: "2.1.0"}},
			semver.Must(semver.NewVersion("2.0.0")),
		},
		{
			map[string]*version.Versions{"a": &version.Versions{Server: "2.1.0"}, "b": &version.Versions{Server: "2.1.0"}, "c": &version.Versions{Server: "2.1.0"}},
			semver.Must(semver.NewVersion("2.1.0")),
		},
		{
			map[string]*version.Versions{"a": nil, "b": &version.Versions{Server: "2.1.0"}, "c": &version.Versions{Server: "2.1.0"}},
			nil,
		},
	}

	for i, tt := range tests {
		dver := decideClusterVersion(tt.vers)
		if !reflect.DeepEqual(dver, tt.wdver) {
			t.Errorf("#%d: ver = %+v, want %+v", i, dver, tt.wdver)
		}
	}
}
