# OrderHub - Event-Driven Microservices

![Go Version](https://img.shields.io/badge/Go-1.21-blue)
![License](https://img.shields.io/badge/License-MIT-green)

A distributed e-commerce order processing system built with Go, demonstrating event sourcing, CQRS, and saga pattern for distributed transactions.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              OrderHub System                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐   │
│  │  Client  │────▶│ Order Service│────▶│ Inventory    │────▶│ Payment  │   │
│  │          │◀────│ (HTTP/gRPC)  │◀────│ Service      │◀────│ Service  │   │
│  └──────────┘     └──────────────┘     └──────────────┘     └──────────┘   │
│                        │                    │                    │           │
│                        ▼                    ▼                    ▼           │
│                  ┌──────────┐         ┌──────────┐         ┌──────────┐    │
│                  │PostgreSQL│         │PostgreSQL│         │ In-Memory│    │
│                  │  Orders  │         │Inventory │         │  (Demo)  │    │
│                  └──────────┘         └──────────┘         └──────────┘    │
│                        │                    │                    │           │
│                        └────────────────────┼────────────────────┘           │
│                                             │                                │
│                                             ▼                                │
│                                    ┌──────────────┐                          │
│                                    │ Apache Kafka │                          │
│                                    │  Event Bus   │                          │
│                                    └──────────────┘                          │
│                                             │                                │
│                        ┌────────────────────┼────────────────────┐           │
│                        ▼                    ▼                    ▼           │
│                  ┌──────────┐         ┌──────────┐         ┌──────────┐    │
│                  │  Redis   │         │Prometheus│         │   Zap    │    │
│                  │  Cache   │         │ Metrics  │         │ Logging  │    │
│                  └──────────┘         └──────────┘         └──────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Features

- **Event-Driven Architecture**: Apache Kafka as the central event bus
- **Saga Pattern**: Distributed transaction management across services
- **CQRS**: Command Query Responsibility Segregation for optimal read/write performance
- **Circuit Breaker**: Resilience pattern for external service calls
- **Idempotency**: Duplicate request handling using Redis
- **Optimistic Locking**: Concurrency control for inventory management
- **Structured Logging**: Zap logger with contextual fields
- **Metrics**: Prometheus endpoints for monitoring
- **Graceful Shutdown**: Proper resource cleanup on termination

## Services

### Order Service
- HTTP API for order management
- Saga orchestrator for distributed transactions
- PostgreSQL for persistence
- Redis for caching and idempotency keys

### Inventory Service
- gRPC server for stock operations
- Event consumer for order events
- Optimistic locking for concurrent updates
- Stock reservation and release logic

### Payment Service
- gRPC server for payment processing
- Idempotent payment handling
- Simulated Stripe integration
- Refund support

## Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- Protobuf compiler (optional, for proto generation)

### Running with Docker Compose

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

### Local Development

```bash
# Install dependencies
make deps

# Generate protobuf files
make proto

# Run tests
make test

# Build all services
make build

# Run individual services
make run-order
make run-inventory
make run-payment
```

## API Documentation

### Order Service (HTTP)

#### Create Order
```bash
POST /api/v1/orders
Content-Type: application/json
X-Idempotency-Key: unique-key-123

{
  "user_id": "user_123",
  "items": [
    {"product_id": "prod_1", "quantity": 2, "price": 1000},
    {"product_id": "prod_2", "quantity": 1, "price": 500}
  ],
  "shipping": {
    "street": "123 Main St",
    "city": "New York",
    "state": "NY",
    "zip_code": "10001",
    "country": "USA"
  }
}
```

#### Get Order
```bash
GET /api/v1/orders/{order_id}
```

#### List Orders
```bash
GET /api/v1/orders?page=1&page_size=20&status=confirmed&user_id=user_123
```

### Health & Metrics

All services expose:
- `/health` - Health check endpoint
- `/metrics` - Prometheus metrics

## Configuration

Configuration can be provided via:
1. YAML config files (`config.yaml`)
2. Environment variables
3. Default values

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_ENV` | Environment (development/production) | development |
| `LOG_LEVEL` | Log level (debug/info/warn/error) | info |
| `SERVER_PORT` | HTTP server port | 8080 |
| `GRPC_PORT` | gRPC server port | 9001 |
| `DB_HOST` | PostgreSQL host | localhost |
| `DB_PORT` | PostgreSQL port | 5432 |
| `REDIS_HOST` | Redis host | localhost |
| `REDIS_PORT` | Redis port | 6379 |
| `KAFKA_BROKERS` | Kafka brokers | localhost:9092 |

## Kubernetes Deployment

```bash
# Create namespace
kubectl create namespace orderhub

# Apply manifests
make k8s-apply

# Or manually
kubectl apply -f k8s/
```

The deployment includes:
- Deployments with HPA (Horizontal Pod Autoscaler)
- Services for internal communication
- Readiness and liveness probes
- Resource limits and requests

## Project Structure

```
.
├── services/
│   ├── order/           # Order service
│   │   ├── main.go
│   │   ├── handlers/
│   │   ├── service/
│   │   ├── repository/
│   │   └── events/
│   ├── inventory/       # Inventory service
│   │   ├── main.go
│   │   ├── service/
│   │   └── repository/
│   └── payment/         # Payment service
│       ├── main.go
│       └── service/
├── shared/              # Shared libraries
│   ├── kafka/
│   ├── circuitbreaker/
│   └── logger/
├── proto/               # Protocol buffers
├── k8s/                 # Kubernetes manifests
├── docker-compose.yml
├── Makefile
└── README.md
```

## Testing

```bash
# Run all tests
make test

# Run with coverage
cd services/order && go test -cover ./...

# Run specific test
go test -run TestCreateOrder ./services/order/service/...
```

## Monitoring

### Prometheus Metrics

Each service exposes metrics at `/metrics`:
- Request latency histograms
- Request counts by status code
- Circuit breaker state
- Database connection pool stats

### Example Queries

```promql
# Request rate
rate(http_requests_total[5m])

# Error rate
rate(http_requests_total{status=~"5.."}[5m])

# P99 latency
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))
```

## License

MIT License - see LICENSE file for details
