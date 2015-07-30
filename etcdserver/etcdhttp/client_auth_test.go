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

package etcdhttp

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/coreos/etcd/etcdserver/auth"
)

const goodPassword = "$2a$10$VYdJecHfm6WNodzv8XhmYeIG4n2SsQefdo5V2t6xIq/aWDHNqSUQW"

func mustJSONRequest(t *testing.T, method string, p string, body string) *http.Request {
	req, err := http.NewRequest(method, path.Join(authPrefix, p), strings.NewReader(body))
	if err != nil {
		t.Fatalf("Error making JSON request: %s %s %s\n", method, p, body)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

type mockAuthStore struct {
	user    *auth.User
	role    *auth.Role
	err     error
	enabled bool
}

func (s *mockAuthStore) AllUsers() ([]string, error)            { return []string{"alice", "bob", "root"}, s.err }
func (s *mockAuthStore) GetUser(name string) (auth.User, error) { return *s.user, s.err }
func (s *mockAuthStore) CreateOrUpdateUser(user auth.User) (out auth.User, created bool, err error) {
	if s.user == nil {
		u, err := s.CreateUser(user)
		return u, true, err
	}
	u, err := s.UpdateUser(user)
	return u, false, err
}
func (s *mockAuthStore) CreateUser(user auth.User) (auth.User, error) { return user, s.err }
func (s *mockAuthStore) DeleteUser(name string) error                 { return s.err }
func (s *mockAuthStore) UpdateUser(user auth.User) (auth.User, error) { return *s.user, s.err }
func (s *mockAuthStore) AllRoles() ([]string, error) {
	return []string{"awesome", "guest", "root"}, s.err
}
func (s *mockAuthStore) GetRole(name string) (auth.Role, error)       { return *s.role, s.err }
func (s *mockAuthStore) CreateRole(role auth.Role) error              { return s.err }
func (s *mockAuthStore) DeleteRole(name string) error                 { return s.err }
func (s *mockAuthStore) UpdateRole(role auth.Role) (auth.Role, error) { return *s.role, s.err }
func (s *mockAuthStore) AuthEnabled() bool                            { return s.enabled }
func (s *mockAuthStore) EnableAuth() error                            { return s.err }
func (s *mockAuthStore) DisableAuth() error                           { return s.err }

func TestAuthFlow(t *testing.T) {
	enableMapMu.Lock()
	enabledMap = make(map[capability]bool)
	enabledMap[authCapability] = true
	enableMapMu.Unlock()
	var testCases = []struct {
		req   *http.Request
		store mockAuthStore

		wcode int
		wbody string
	}{
		{
			req:   mustJSONRequest(t, "PUT", "users/alice", `{{{{{{{`),
			store: mockAuthStore{},
			wcode: http.StatusBadRequest,
			wbody: `{"message":"Invalid JSON in request body."}`,
		},
		{
			req:   mustJSONRequest(t, "PUT", "users/alice", `{"user": "alice", "password": "goodpassword"}`),
			store: mockAuthStore{enabled: true},
			wcode: http.StatusUnauthorized,
			wbody: `{"message":"Insufficient credentials"}`,
		},
		// Users
		{
			req:   mustJSONRequest(t, "GET", "users", ""),
			store: mockAuthStore{},
			wcode: http.StatusOK,
			wbody: `{"users":["alice","bob","root"]}`,
		},
		{
			req: mustJSONRequest(t, "GET", "users/alice", ""),
			store: mockAuthStore{
				user: &auth.User{
					User:     "alice",
					Roles:    []string{"alicerole", "guest"},
					Password: "wheeee",
				},
			},
			wcode: http.StatusOK,
			wbody: `{"user":"alice","roles":["alicerole","guest"]}`,
		},
		{
			req:   mustJSONRequest(t, "PUT", "users/alice", `{"user": "alice", "password": "goodpassword"}`),
			store: mockAuthStore{},
			wcode: http.StatusCreated,
			wbody: `{"user":"alice","roles":null}`,
		},
		{
			req:   mustJSONRequest(t, "DELETE", "users/alice", ``),
			store: mockAuthStore{},
			wcode: http.StatusOK,
			wbody: ``,
		},
		{
			req: mustJSONRequest(t, "PUT", "users/alice", `{"user": "alice", "password": "goodpassword"}`),
			store: mockAuthStore{
				user: &auth.User{
					User:     "alice",
					Roles:    []string{"alicerole", "guest"},
					Password: "wheeee",
				},
			},
			wcode: http.StatusOK,
			wbody: `{"user":"alice","roles":["alicerole","guest"]}`,
		},
		{
			req: mustJSONRequest(t, "PUT", "users/alice", `{"user": "alice", "grant": ["alicerole"]}`),
			store: mockAuthStore{
				user: &auth.User{
					User:     "alice",
					Roles:    []string{"alicerole", "guest"},
					Password: "wheeee",
				},
			},
			wcode: http.StatusOK,
			wbody: `{"user":"alice","roles":["alicerole","guest"]}`,
		},
		{
			req: mustJSONRequest(t, "GET", "users/alice", ``),
			store: mockAuthStore{
				user: &auth.User{},
				err:  auth.Error{Status: http.StatusNotFound, Errmsg: "auth: User alice doesn't exist."},
			},
			wcode: http.StatusNotFound,
			wbody: `{"message":"auth: User alice doesn't exist."}`,
		},
		// Roles
		{
			req: mustJSONRequest(t, "GET", "roles/manager", ""),
			store: mockAuthStore{
				role: &auth.Role{
					Role: "manager",
				},
			},
			wcode: http.StatusOK,
			wbody: `{"role":"manager","permissions":{"kv":{"read":null,"write":null}}}`,
		},
		{
			req:   mustJSONRequest(t, "DELETE", "roles/manager", ``),
			store: mockAuthStore{},
			wcode: http.StatusOK,
			wbody: ``,
		},
		{
			req:   mustJSONRequest(t, "PUT", "roles/manager", `{"role":"manager","permissions":{"kv":{"read":[],"write":[]}}}`),
			store: mockAuthStore{},
			wcode: http.StatusCreated,
			wbody: `{"role":"manager","permissions":{"kv":{"read":[],"write":[]}}}`,
		},
		{
			req: mustJSONRequest(t, "PUT", "roles/manager", `{"role":"manager","revoke":{"kv":{"read":["foo"],"write":[]}}}`),
			store: mockAuthStore{
				role: &auth.Role{
					Role: "manager",
				},
			},
			wcode: http.StatusOK,
			wbody: `{"role":"manager","permissions":{"kv":{"read":null,"write":null}}}`,
		},
		{
			req:   mustJSONRequest(t, "GET", "roles", ""),
			store: mockAuthStore{},
			wcode: http.StatusOK,
			wbody: `{"roles":["awesome","guest","root"]}`,
		},
		{
			req: mustJSONRequest(t, "GET", "enable", ""),
			store: mockAuthStore{
				enabled: true,
			},
			wcode: http.StatusOK,
			wbody: `{"enabled":true}`,
		},
		{
			req: mustJSONRequest(t, "PUT", "enable", ""),
			store: mockAuthStore{
				enabled: false,
			},
			wcode: http.StatusOK,
			wbody: ``,
		},
		{
			req: (func() *http.Request {
				req := mustJSONRequest(t, "DELETE", "enable", "")
				req.SetBasicAuth("root", "good")
				return req
			})(),
			store: mockAuthStore{
				enabled: true,
				user: &auth.User{
					User:     "root",
					Password: goodPassword,
					Roles:    []string{"root"},
				},
				role: &auth.Role{
					Role: "root",
				},
			},
			wcode: http.StatusOK,
			wbody: ``,
		},
	}

	for i, tt := range testCases {
		mux := http.NewServeMux()
		h := &authHandler{
			sec:     &tt.store,
			cluster: &fakeCluster{id: 1},
		}
		handleAuth(mux, h)
		rw := httptest.NewRecorder()
		mux.ServeHTTP(rw, tt.req)
		if rw.Code != tt.wcode {
			t.Errorf("#%d: got code=%d, want %d", i, rw.Code, tt.wcode)
		}
		g := rw.Body.String()
		g = strings.TrimSpace(g)
		if g != tt.wbody {
			t.Errorf("#%d: got body=%s, want %s", i, g, tt.wbody)
		}
	}
}

func mustAuthRequest(method, username, password string) *http.Request {
	req, err := http.NewRequest(method, "path", strings.NewReader(""))
	if err != nil {
		panic("Cannot make auth request: " + err.Error())
	}
	req.SetBasicAuth(username, password)
	return req
}

func TestPrefixAccess(t *testing.T) {
	var table = []struct {
		key                string
		req                *http.Request
		store              *mockAuthStore
		hasRoot            bool
		hasKeyPrefixAccess bool
		hasRecursiveAccess bool
	}{
		{
			key: "/foo",
			req: mustAuthRequest("GET", "root", "good"),
			store: &mockAuthStore{
				user: &auth.User{
					User:     "root",
					Password: goodPassword,
					Roles:    []string{"root"},
				},
				role: &auth.Role{
					Role: "root",
				},
				enabled: true,
			},
			hasRoot:            true,
			hasKeyPrefixAccess: true,
			hasRecursiveAccess: true,
		},
		{
			key: "/foo",
			req: mustAuthRequest("GET", "user", "good"),
			store: &mockAuthStore{
				user: &auth.User{
					User:     "user",
					Password: goodPassword,
					Roles:    []string{"foorole"},
				},
				role: &auth.Role{
					Role: "foorole",
					Permissions: auth.Permissions{
						KV: auth.RWPermission{
							Read:  []string{"/foo"},
							Write: []string{"/foo"},
						},
					},
				},
				enabled: true,
			},
			hasRoot:            false,
			hasKeyPrefixAccess: true,
			hasRecursiveAccess: false,
		},
		{
			key: "/foo",
			req: mustAuthRequest("GET", "user", "good"),
			store: &mockAuthStore{
				user: &auth.User{
					User:     "user",
					Password: goodPassword,
					Roles:    []string{"foorole"},
				},
				role: &auth.Role{
					Role: "foorole",
					Permissions: auth.Permissions{
						KV: auth.RWPermission{
							Read:  []string{"/foo*"},
							Write: []string{"/foo*"},
						},
					},
				},
				enabled: true,
			},
			hasRoot:            false,
			hasKeyPrefixAccess: true,
			hasRecursiveAccess: true,
		},
		{
			key: "/foo",
			req: mustAuthRequest("GET", "user", "bad"),
			store: &mockAuthStore{
				user: &auth.User{
					User:     "user",
					Password: goodPassword,
					Roles:    []string{"foorole"},
				},
				role: &auth.Role{
					Role: "foorole",
					Permissions: auth.Permissions{
						KV: auth.RWPermission{
							Read:  []string{"/foo*"},
							Write: []string{"/foo*"},
						},
					},
				},
				enabled: true,
			},
			hasRoot:            false,
			hasKeyPrefixAccess: false,
			hasRecursiveAccess: false,
		},
		{
			key: "/foo",
			req: mustAuthRequest("GET", "user", "good"),
			store: &mockAuthStore{
				user:    &auth.User{},
				err:     errors.New("Not the user"),
				enabled: true,
			},
			hasRoot:            false,
			hasKeyPrefixAccess: false,
			hasRecursiveAccess: false,
		},
		{
			key: "/foo",
			req: mustJSONRequest(t, "GET", "somepath", ""),
			store: &mockAuthStore{
				user: &auth.User{
					User:     "user",
					Password: goodPassword,
					Roles:    []string{"foorole"},
				},
				role: &auth.Role{
					Role: "guest",
					Permissions: auth.Permissions{
						KV: auth.RWPermission{
							Read:  []string{"/foo*"},
							Write: []string{"/foo*"},
						},
					},
				},
				enabled: true,
			},
			hasRoot:            false,
			hasKeyPrefixAccess: true,
			hasRecursiveAccess: true,
		},
		{
			key: "/bar",
			req: mustJSONRequest(t, "GET", "somepath", ""),
			store: &mockAuthStore{
				user: &auth.User{
					User:     "user",
					Password: goodPassword,
					Roles:    []string{"foorole"},
				},
				role: &auth.Role{
					Role: "guest",
					Permissions: auth.Permissions{
						KV: auth.RWPermission{
							Read:  []string{"/foo*"},
							Write: []string{"/foo*"},
						},
					},
				},
				enabled: true,
			},
			hasRoot:            false,
			hasKeyPrefixAccess: false,
			hasRecursiveAccess: false,
		},
	}

	for i, tt := range table {
		if tt.hasRoot != hasRootAccess(tt.store, tt.req) {
			t.Errorf("#%d: hasRoot doesn't match (expected %v)", i, tt.hasRoot)
		}
		if tt.hasKeyPrefixAccess != hasKeyPrefixAccess(tt.store, tt.req, tt.key, false) {
			t.Errorf("#%d: hasKeyPrefixAccess doesn't match (expected %v)", i, tt.hasRoot)
		}
		if tt.hasRecursiveAccess != hasKeyPrefixAccess(tt.store, tt.req, tt.key, true) {
			t.Errorf("#%d: hasRecursiveAccess doesn't match (expected %v)", i, tt.hasRoot)
		}
	}
}
