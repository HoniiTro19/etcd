// Copyright 2023 Huidong Zhang, OceanBase, AntGroup
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

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/raft/v3/raftpb"
)

func main() {
	root, err := os.Getwd()
	if err != nil {
		fmt.Println("fail to get working directory")
	}

	// user input parameters to configure the election experiment
	basedir := flag.String("basedir", root, "directory to output experiment logs")
	desc := flag.String("desc", "example", "concise description about the name and configurations of this experiment")
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "member index in the cluster peers")
	duration := flag.Duration("duration", 60, "alive duration of the raft instance in seconds")
	latency := flag.Int("latency", 0, "average latency of the real network condition")
	mocknet := flag.Bool("mocknet", false, "use mock network module to simulate message latency and loss")
	msgloss := flag.Int("msgloss", 0, "ratio to trigger message loss in percentage (only works when mocknet is true)")
	msgdelay := flag.Int("msgdelay", 0, "additional latency for message transmission (only works when mocknet is true)")
	leadkill := flag.Bool("leadkill", false, "kill the leader after the first round of election and observe the time to repair")
	flag.Parse()

	// configure the zap logger
	path := filepath.Join(*basedir, *desc, time.Now().Format("2006-01-02 15:04:05"), fmt.Sprintf("%d", *id))
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		panic(fmt.Sprintf("fail to create result path (%s), error (%v)", path, err))
	}
	logger := zap.New(getNewCore(filepath.Join(path, "election.log")))

	// start the mock network and raft node
	var inQueueC chan<- raftpb.Message
	var outQueueC <-chan []raftpb.Message
	stopc := make(chan struct{})
	if *mocknet {
		inQueueC, outQueueC = newMockNet(*msgloss, *msgdelay, stopc, logger)
	}
	args := &Args{
		id:        *id,
		peers:     strings.Split(*cluster, ","),
		latency:   *latency,
		inQueueC:  inQueueC,
		outQueueC: outQueueC,
		leadkill:  *leadkill,
	}
	stopDoneC := newRaftNode(args, logger)

	// wait util experiment timeout and the raftNode stopped
	if _, ok := <-time.After(*duration * time.Second); ok {
		close(stopc)
	}
	if _, ok := <-stopDoneC; !ok {
		logger.Info("election instance stopped", zap.Int("member", *id))
	}
}
