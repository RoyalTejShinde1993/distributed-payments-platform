.PHONY: all build run proto tidy

all: build

build:
	go build -o bin/payment-service ./cmd/payment-service

run:
	go run ./cmd/payment-service

proto:
	protoc --go_out=. --go-grpc_out=. --proto_path=pkg/api/payment/v1 pkg/api/payment/v1/payment.proto

 tidy:
	go mod tidy
