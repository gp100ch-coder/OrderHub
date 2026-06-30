# OrderHub - Event-Driven Microservices

Language: Go

## Description
Distributed e-commerce order processing system with event sourcing, 
CQRS, and saga pattern for distributed transactions.
Demonstrates: system design, distributed systems, event-driven architecture.

## Dependencies
- github.com/gin-gonic/gin
- github.com/confluentinc/confluent-kafka-go/v2
- github.com/redis/go-redis/v9
- github.com/jackc/pgx/v5
- github.com/sony/gobreaker
- github.com/uber-go/zap
- github.com/prometheus/client_golang
- github.com/grpc-ecosystem/go-grpc-middleware
- google.golang.org/grpc
- google.golang.org/protobuf
- github.com/spf13/viper
- github.com/golang-migrate/migrate/v4
- github.com/stretchr/testify

## Requirements
- 3 microservices: Order, Inventory, Payment (separate Go modules)
- gRPC for inter-service communication
- Apache Kafka for event bus
- PostgreSQL per service (database per service pattern)
- Redis for caching and idempotency keys
- Circuit breaker pattern for external calls
- Structured logging with Zap
- Prometheus metrics endpoint
- Distributed tracing with OpenTelemetry
- Graceful shutdown handling
- Docker Compose for local development
- Kubernetes manifests for deployment

## Files

### File: `services/order/main.go`
Order service entry point with graceful shutdown, config loading

### File: `services/order/handlers/order_handler.go`
Gin HTTP handlers: create order, get order, list orders

### File: `services/order/service/order_service.go`
Business logic with saga orchestrator for distributed transaction

### File: `services/order/repository/order_repo.go`
PostgreSQL repository with pgx, transaction support

### File: `services/order/events/publisher.go`
Kafka producer for order events with delivery guarantees

### File: `services/inventory/main.go`
Inventory service with gRPC server, event consumer

### File: `services/inventory/service/inventory_service.go`
Stock reservation, release, and fulfillment logic

### File: `services/inventory/repository/inventory_repo.go`
Inventory CRUD with optimistic locking

### File: `services/payment/main.go`
Payment service with Stripe integration simulation

### File: `services/payment/service/payment_service.go`
Payment processing with idempotency and refund support

### File: `proto/order.proto`
Protobuf definitions for gRPC services and messages

### File: `shared/kafka/consumer.go`
Generic Kafka consumer with retry, dead-letter queue

### File: `shared/circuitbreaker/breaker.go`
Circuit breaker implementation with half-open state

### File: `shared/logger/logger.go`
Zap logger with contextual fields, request tracing

### File: `docker-compose.yml`
Kafka, Zookeeper, PostgreSQL x3, Redis, all services

### File: `k8s/order-deployment.yaml`
Kubernetes deployment with HPA, readiness/liveness probes

### File: `Makefile`
Build, test, proto generation, migration commands

### File: `README.md`
Architecture diagram, API docs, deployment guide
