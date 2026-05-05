# Distributed Payments Platform

Distributed Payments Platform is a Go-based starter repository for building a payment orchestration service using Kafka and gRPC.

## Overview

- `Go` service skeleton for payment processing
- `Kafka` producer/consumer transport
- `gRPC` server bootstrap
- Protocol buffer contract under `pkg/api/payment/v1`

## Project layout

- `cmd/payment-service` - application entrypoint
- `internal/payments` - domain orchestration and service lifecycle
- `internal/payments/transport/kafka` - Kafka producer / consumer
- `internal/payments/transport/grpc` - gRPC server bootstrap
- `pkg/api/payment/v1` - protobuf API definitions

## Getting started

1. Start Kafka and ZooKeeper:

```bash
docker compose up -d
```

2. Run the service:

```bash
go run ./cmd/payment-service
```

3. Build the CLI binary:

```bash
make build
```

## Protobuf generation

If you have `protoc` installed, generate Go code from the service contract:

```bash
make proto
```

## Notes

- The current scaffold provides the platform structure and transport abstractions.
- The payment service can be expanded with gRPC handlers, persistence, and payment orchestration logic.

## Screenshot

Add a screenshot of the running HTTP gateway or service UI here. Place the image at `assets/screenshot.png` in the repository and it will be displayed below.

![Service screenshot](assets/screenshot.png)
