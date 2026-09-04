.PHONY: build test lint proto

build:
	go build ./...

test:
	go test ./... -race

lint:
	golangci-lint run

# Requires protoc, protoc-gen-go, and protoc-gen-go-grpc on PATH.
# Adjust the second -I if your protoc install keeps google/protobuf/*.proto elsewhere.
proto:
	protoc -I api -I "$$(go env GOPATH)"/../protoc/include \
		--go_out=. --go_opt=module=github.com/maxider/job-queue \
		--go-grpc_out=. --go-grpc_opt=module=github.com/maxider/job-queue \
		api/jobqueue/v1/jobqueue.proto
