#!/bin/sh

PATH=${PATH}:${GOBIN}
GOGOPROTO_ROOT="${GOPATH}/src"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

protoc --gogofaster_out=plugins=grpc:. -I. -I${GOPATH}/src -I${GOPATH}/src/github.com/gogo/protobuf/protobuf *.proto
#protoc --gogofaster_out=plugins=grpc:. -I=.:"${GOGOPROTO_PATH}" *.proto
