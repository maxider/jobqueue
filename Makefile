.PHONY: build test lint proto kind-up kind-down

KIND_CLUSTER := job-queue
KEDA_VERSION := v2.20.2

build:
	cd go && go build ./...

test:
	cd go && go test ./... -race

lint:
	cd go && golangci-lint run

# Requires protoc, protoc-gen-go, and protoc-gen-go-grpc on PATH.
# Adjust the second -I if your protoc install keeps google/protobuf/*.proto elsewhere.
proto:
	protoc -I api -I "$$(cd go && go env GOPATH)"/../protoc/include \
		--go_out=go --go_opt=module=github.com/maxider/job-queue \
		--go-grpc_out=go --go-grpc_opt=module=github.com/maxider/job-queue \
		api/jobqueue/v1/jobqueue.proto

# Requires kubectl, kind, and docker on PATH. Brings up a local kind
# cluster, installs KEDA, builds+loads the image, and applies deploy/k8s/
# (including a Grafana instance whose datasource/dashboard ConfigMaps are
# generated here from deploy/grafana/provisioning/ so that content has one
# source of truth shared with docker-compose). Safe to re-run.
kind-up:
	kind create cluster --name $(KIND_CLUSTER) 2>/dev/null || true
	kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/$(KEDA_VERSION)/keda-$(KEDA_VERSION:v%=%).yaml
	kubectl -n keda wait --for=condition=Available deployment --all --timeout=120s
	docker build -f deploy/Dockerfile -t jobqueue:dev .
	kind load docker-image jobqueue:dev --name $(KIND_CLUSTER)
	kubectl apply -f deploy/k8s/
	kubectl -n job-queue create configmap grafana-datasources \
		--from-file=deploy/grafana/provisioning/datasources/prometheus.yml \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl -n job-queue create configmap grafana-dashboards \
		--from-file=deploy/grafana/provisioning/dashboards/dashboards.yml \
		--from-file=deploy/grafana/provisioning/dashboards/job-queue.json \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl -n job-queue rollout status deployment/server --timeout=60s
	kubectl -n job-queue rollout status deployment/grafana --timeout=60s

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)
