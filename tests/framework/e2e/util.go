// Copyright 2017 The etcd Authors
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

package e2e

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.etcd.io/etcd/pkg/v3/expect"
)

func WaitReadyExpectProc(exproc *expect.ExpectProcess, readyStrs []string) error {
	matchSet := func(l string) bool {
		for _, s := range readyStrs {
			if strings.Contains(l, s) {
				return true
			}
		}
		return false
	}
	_, err := exproc.ExpectFunc(matchSet)
	return err
}

func SpawnWithExpect(args []string, expected string) error {
	return SpawnWithExpects(args, nil, []string{expected}...)
}

func SpawnWithExpectWithEnv(args []string, envVars map[string]string, expected string) error {
	return SpawnWithExpects(args, envVars, []string{expected}...)
}

func SpawnWithExpects(args []string, envVars map[string]string, xs ...string) error {
	_, err := SpawnWithExpectLines(args, envVars, xs...)
	return err
}

func SpawnWithExpectLines(args []string, envVars map[string]string, xs ...string) ([]string, error) {
	proc, err := SpawnCmd(args, envVars)
	if err != nil {
		return nil, err
	}
	defer proc.Close()
	// process until either stdout or stderr contains
	// the expected string
	var (
		lines []string
	)
	for _, txt := range xs {
		l, lerr := proc.Expect(txt)
		if lerr != nil {
			proc.Close()
			return nil, fmt.Errorf("%v %v (expected %q, got %q). Try EXPECT_DEBUG=TRUE", args, lerr, txt, lines)
		}
		lines = append(lines, l)
	}
	perr := proc.Close()
	l := proc.LineCount()
	if len(xs) == 0 && l != noOutputLineCount { // expect no output
		return nil, fmt.Errorf("unexpected output from %v (got lines %q, line count %d) %v. Try EXPECT_DEBUG=TRUE", args, lines, l, l != noOutputLineCount)
	}
	return lines, perr
}

func RandomLeaseID() int64 {
	return rand.New(rand.NewSource(time.Now().UnixNano())).Int63()
}

func DataMarshal(data interface{}) (d string, e error) {
	m, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

func CloseWithTimeout(p *expect.ExpectProcess, d time.Duration) error {
	errc := make(chan error, 1)
	go func() { errc <- p.Close() }()
	select {
	case err := <-errc:
		return err
	case <-time.After(d):
		p.Stop()
		// retry close after stopping to collect SIGQUIT data, if any
		CloseWithTimeout(p, time.Second)
	}
	return fmt.Errorf("took longer than %v to Close process %+v", d, p)
}

func ToTLS(s string) string {
	return strings.Replace(s, "http://", "https://", 1)
}

func SkipInShortMode(t testing.TB) {
	testutil.SkipTestIfShortMode(t, "e2e tests are not running in --short mode")
}

func mergeEnvVariables(envVars map[string]string) []string {
	var env []string
	// Environment variables are passed as parameter have higher priority
	// than os environment variables.
	for k, v := range envVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	// Now, we can set os environment variables not passed as parameter.
	currVars := os.Environ()
	for _, v := range currVars {
		p := strings.Split(v, "=")
		if _, ok := envVars[p[0]]; !ok {
			env = append(env, fmt.Sprintf("%s=%s", p[0], p[1]))
		}
	}

	return env
}
