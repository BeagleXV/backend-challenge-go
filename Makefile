.PHONY: run build test test-race test-integration vet fmt migrate-up migrate-down

run:
	go run ./cmd/api

build:
	go build -o bin/api ./cmd/api

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	go test -tags=integration ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

migrate-up:
	migrate -path migrations -database "$${DATABASE_URL}" up

migrate-down:
	migrate -path migrations -database "$${DATABASE_URL}" down 1
