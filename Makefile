.PHONY: run simulate test vet fmt check docker-up docker-down

run:
	go run ./cmd/api

simulate:
	go run ./cmd/simulator

test:
	go test -race -cover ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test -race ./...
	go build ./cmd/...

docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

