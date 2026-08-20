#!/bin/sh
# Developer tooling helpers for machines without Homebrew/admin.
# Usage: deploy/tools.sh <install-protoc|gen-proto>
set -eu

PROTOC_VERSION=36.0
PROTOC_DIR="$HOME/sdk/protoc"

cmd=${1:-}
case "$cmd" in
install-protoc)
    mkdir -p "$PROTOC_DIR"
    cd "$PROTOC_DIR"
    curl -sSLO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-aarch_64.zip"
    unzip -oq "protoc-${PROTOC_VERSION}-osx-aarch_64.zip"
    "$PROTOC_DIR/bin/protoc" --version
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    ;;
gen-proto)
    PATH="$HOME/go/bin:$PATH" "$PROTOC_DIR/bin/protoc" \
        --proto_path=pkg/rpc \
        --go_out=pkg/rpc/marshalpb --go_opt=paths=source_relative \
        --go-grpc_out=pkg/rpc/marshalpb --go-grpc_opt=paths=source_relative \
        pkg/rpc/marshal.proto
    ;;
*)
    echo "usage: deploy/tools.sh <install-protoc|gen-proto>" >&2
    exit 2
    ;;
esac
