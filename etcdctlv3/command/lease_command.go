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

package command

import (
	"fmt"
	"os"
	"strconv"

	"github.com/coreos/etcd/Godeps/_workspace/src/github.com/spf13/cobra"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

// NewLeaseCommand returns the cobra command for "lease".
func NewLeaseCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "lease",
		Short: "lease is used to manage leases.",
	}

	lc.AddCommand(NewLeaseCreateCommand())
	lc.AddCommand(NewLeaseRevokeCommand())

	return lc
}

// NewLeaseCreateCommand returns the cobra command for "lease create".
func NewLeaseCreateCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "create",
		Short: "create is used to create leases.",

		Run: leaseCreateCommandFunc,
	}

	return lc
}

// leaseCreateCommandFunc executes the "lease create" command.
func leaseCreateCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("lease create command needs TTL arguement."))
	}

	ttl, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		ExitWithError(ExitBadArgs, fmt.Errorf("bad TTL (%v)", err))
	}

	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		ExitWithError(ExitError, err)
	}
	conn, err := grpc.Dial(endpoint)
	if err != nil {
		ExitWithError(ExitBadConnection, err)
	}
	lease := pb.NewLeaseClient(conn)

	req := &pb.LeaseCreateRequest{TTL: ttl}
	resp, err := lease.LeaseCreate(context.Background(), req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create lease (%v)\n", err)
		return
	}
	fmt.Printf("lease %016x created with TTL(%ds)\n", resp.ID, resp.TTL)
}

// NewLeaseRevokeCommand returns the cobra command for "lease revoke".
func NewLeaseRevokeCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "revoke",
		Short: "revoke is used to revoke leases.",

		Run: leaseRevokeCommandFunc,
	}

	return lc
}

// leaseRevokeCommandFunc executes the "lease create" command.
func leaseRevokeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("lease revoke command needs 1 argument"))
	}

	id, err := strconv.ParseInt(args[0], 16, 64)
	if err != nil {
		ExitWithError(ExitBadArgs, fmt.Errorf("bad lease ID arg (%v), expecting ID in Hex", err))
	}

	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		ExitWithError(ExitError, err)
	}
	conn, err := grpc.Dial(endpoint)
	if err != nil {
		ExitWithError(ExitBadConnection, err)
	}
	lease := pb.NewLeaseClient(conn)

	req := &pb.LeaseRevokeRequest{ID: id}
	_, err = lease.LeaseRevoke(context.Background(), req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to revoke lease (%v)\n", err)
		return
	}
	fmt.Printf("lease %016x revoked\n", id)
}
