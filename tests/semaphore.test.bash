#!/usr/bin/env bash

if ! [[ "$0" =~ "tests/semaphore.test.bash" ]]; then
  echo "must be run from repository root"
  exit 255
fi

<<COMMENT
# amd64-e2e
bash tests/semaphore.test.bash

# 386-e2e
TEST_ARCH=386 bash tests/semaphore.test.bash

# grpc-proxy
TEST_OPTS="PASSES='build grpcproxy'" bash tests/semaphore.test.bash

# coverage
TEST_OPTS="coverage" bash tests/semaphore.test.bash
COMMENT

TEST_SUFFIX=$(date +%s | base64 | head -c 15)

if [ -z "${TEST_OPTS}" ]; then
	TEST_OPTS="PASSES='build release e2e' MANUAL_VER=v3.3.7"
fi
if [ "${TEST_ARCH}" == "386" ]; then
  TEST_OPTS="GOARCH=386 PASSES='build e2e'"
fi

echo "Running tests with" ${TEST_OPTS}
if [ "${TEST_OPTS}" == "coverage" ]; then
  make docker-test-coverage
else
  docker run \
    --rm \
    --volume=`pwd`:/go/src/github.com/coreos/etcd \
    gcr.io/etcd-development/etcd-test:go1.10.3 \
    /bin/bash -c "${TEST_OPTS} ./test 2>&1 | tee test-${TEST_SUFFIX}.log"

  ! egrep "(--- FAIL:|DATA RACE|panic: test timed out|appears to have leaked)" -B50 -A10 test-${TEST_SUFFIX}.log
fi
