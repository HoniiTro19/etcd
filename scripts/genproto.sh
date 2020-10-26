#!/usr/bin/env bash
#
# Generate all etcd protobuf bindings.
# Run from repository root.
#
set -e

if ! [[ "$0" =~ scripts/genproto.sh ]]; then
  echo "must be run from repository root"
  exit 255
fi

source ./scripts/test_lib.sh

if [[ $(protoc --version | cut -f2 -d' ') != "3.12.3" ]]; then
  echo "could not find protoc 3.12.3, is it installed + in PATH?"
  exit 255
fi

run env GO111MODULE=off go get -u github.com/myitcv/gobin

GOFAST_BIN=$(tool_get_bin github.com/gogo/protobuf/protoc-gen-gofast)
GRPC_GATEWAY_BIN=$(tool_get_bin github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway)
SWAGGER_BIN=$(tool_get_bin github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger)
GOGOPROTO_ROOT="$(tool_pkg_dir github.com/gogo/protobuf/proto)/.."
GRPC_GATEWAY_ROOT="$(tool_pkg_dir github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway)/.."

echo
echo "Resolved binary and packages versions:"
echo "  - protoc-gen-gofast:       ${GOFAST_BIN}"
echo "  - protoc-gen-grpc-gateway: ${GRPC_GATEWAY_BIN}"
echo "  - swagger:                 ${SWAGGER_BIN}"
echo "  - gogoproto-root:          ${GOGOPROTO_ROOT}"
echo "  - grpc-gateway-root:       ${GRPC_GATEWAY_ROOT}"

# directories containing protos to be built
DIRS="./wal/walpb ./api/etcdserverpb ./etcdserver/api/snap/snappb ./raft/raftpb ./api/mvccpb ./lease/leasepb ./api/authpb ./etcdserver/api/v3lock/v3lockpb ./etcdserver/api/v3election/v3electionpb ./api/membershippb"

GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

log_callout -e "\nRunning gofast proto generation..."

for dir in ${DIRS}; do
  run pushd "${dir}"
    run protoc --gofast_out=plugins=grpc:. -I=".:${GOGOPROTO_PATH}:${ETCD_ROOT_DIR}/..:${GRPC_GATEWAY_ROOT}/third_party/googleapis" \
      --plugin="${GOFAST_BIN}" ./*.proto

    sed -i.bak -E 's|"etcd/api/|"go.etcd.io/etcd/api/v3/|g' ./*.pb.go

    rm -f ./*.bak
    run gofmt -s -w ./*.pb.go
    run goimports -w ./*.pb.go
  run popd
done

#return
log_callout -e "\nRunning swagger & grpc_gateway proto generation..."

# remove old swagger files so it's obvious whether the files fail to generate
rm -rf Documentation/dev-guide/apispec/swagger/*json
for pb in api/etcdserverpb/rpc etcdserver/api/v3lock/v3lockpb/v3lock etcdserver/api/v3election/v3electionpb/v3election; do
  log_callout "grpc & swagger for: ${pb}.proto"
  run protoc -I. \
      -I"${GRPC_GATEWAY_ROOT}"/third_party/googleapis \
      -I"${GOGOPROTO_PATH}" \
      -I"${ETCD_ROOT_DIR}/.." \
      --grpc-gateway_out=logtostderr=true,paths=source_relative:. \
      --swagger_out=logtostderr=true:./Documentation/dev-guide/apispec/swagger/. \
      --plugin="${SWAGGER_BIN}" --plugin="${GRPC_GATEWAY_BIN}" \
      ${pb}.proto
  # hack to move gw files around so client won't include them
  pkgpath=$(dirname "${pb}")
  pkg=$(basename "${pkgpath}")
  gwfile="${pb}.pb.gw.go"

  sed -i -E "s#package $pkg#package gw#g" "${gwfile}"
  sed -i -E "s#import \(#import \(\"go.etcd.io/etcd/${pkgpath}\"#g" "${gwfile}"
  sed -i -E "s#([ (])([a-zA-Z0-9_]*(Client|Server|Request)([^(]|$))#\1${pkg}.\2#g" "${gwfile}"
  sed -i -E "s# (New[a-zA-Z0-9_]*Client\()# ${pkg}.\1#g" "${gwfile}"
  sed -i -E "s|go.etcd.io/etcd|go.etcd.io/etcd/v3|g" "${gwfile}"
  sed -i -E "s|go.etcd.io/etcd/v3/api|go.etcd.io/etcd/api/v3|g" "${gwfile}"
  
  run go fmt "${gwfile}"

  gwdir="${pkgpath}/gw/"
  run mkdir -p "${gwdir}"
  run mv "${gwfile}" "${gwdir}"

  swaggerName=$(basename ${pb})
  run mv  Documentation/dev-guide/apispec/swagger/${pb}.swagger.json \
    Documentation/dev-guide/apispec/swagger/"${swaggerName}".swagger.json
done

log_callout -e "\nRunning swagger ..."
run_go_tool github.com/hexfusion/schwag -input=Documentation/dev-guide/apispec/swagger/rpc.swagger.json

if [ "$1" != "--skip-protodoc" ]; then
  log_callout "protodoc is auto-generating grpc API reference documentation..."

  run rm -rf Documentation/dev-guide/api_reference_v3.md
  run_go_tool go.etcd.io/protodoc --directories="api/etcdserverpb=service_message,api/mvccpb=service_message,lease/leasepb=service_message,api/authpb=service_message" \
    --title="etcd API Reference" \
    --output="Documentation/dev-guide/api_reference_v3.md" \
    --message-only-from-this-file="api/etcdserverpb/rpc.proto" \
    --disclaimer="This is a generated documentation. Please read the proto files for more." || exit 2

  run rm -rf Documentation/dev-guide/api_concurrency_reference_v3.md
  run_go_tool go.etcd.io/protodoc --directories="etcdserver/api/v3lock/v3lockpb=service_message,etcdserver/api/v3election/v3electionpb=service_message,api/mvccpb=service_message" \
    --title="etcd concurrency API Reference" \
    --output="Documentation/dev-guide/api_concurrency_reference_v3.md" \
    --disclaimer="This is a generated documentation. Please read the proto files for more." || exit 2

  log_success "protodoc is finished."
else
  log_warning "skipping grpc API reference document auto-generation..."
fi

log_success -e "\n./genproto SUCCESS"
