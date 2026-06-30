.PHONY: build test clean proto migrate run docker k8s-lint

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=orderhub

# Directories
SERVICES_DIR=services
ORDER_SERVICE=$(SERVICES_DIR)/order
INVENTORY_SERVICE=$(SERVICES_DIR)/inventory
PAYMENT_SERVICE=$(SERVICES_DIR)/payment
SHARED_DIR=shared
PROTO_DIR=proto

# Build all services
build: build-order build-inventory build-payment

build-order:
	@echo "Building Order Service..."
	cd $(ORDER_SERVICE) && $(GOBUILD) -o ../../bin/order-service ./...

build-inventory:
	@echo "Building Inventory Service..."
	cd $(INVENTORY_SERVICE) && $(GOBUILD) -o ../../bin/inventory-service ./...

build-payment:
	@echo "Building Payment Service..."
	cd $(PAYMENT_SERVICE) && $(GOBUILD) -o ../../bin/payment-service ./...

# Run tests for all services
test: test-order test-inventory test-payment

test-order:
	@echo "Testing Order Service..."
	cd $(ORDER_SERVICE) && $(GOTEST) -v -race -cover ./...

test-inventory:
	@echo "Testing Inventory Service..."
	cd $(INVENTORY_SERVICE) && $(GOTEST) -v -race -cover ./...

test-payment:
	@echo "Testing Payment Service..."
	cd $(PAYMENT_SERVICE) && $(GOTEST) -v -race -cover ./...

# Generate protobuf files
proto:
	@echo "Generating protobuf files..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/order.proto
	mkdir -p proto/pb
	mv $(PROTO_DIR)/*.pb.go proto/pb/ 2>/dev/null || true

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	cd $(SHARED_DIR) && $(GOMOD) download
	cd $(ORDER_SERVICE) && $(GOMOD) download
	cd $(INVENTORY_SERVICE) && $(GOMOD) download
	cd $(PAYMENT_SERVICE) && $(GOMOD) download

# Tidy up go.mod files
tidy:
	@echo "Tidying modules..."
	cd $(SHARED_DIR) && $(GOMOD) tidy
	cd $(ORDER_SERVICE) && $(GOMOD) tidy
	cd $(INVENTORY_SERVICE) && $(GOMOD) tidy
	cd $(PAYMENT_SERVICE) && $(GOMOD) tidy

# Run database migrations
migrate: migrate-order migrate-inventory migrate-payment

migrate-order:
	@echo "Running Order Service migrations..."
	migrate -path $(ORDER_SERVICE)/migrations -database "postgres://orderhub:orderhub_secret@localhost:5432/orders?sslmode=disable" up

migrate-inventory:
	@echo "Running Inventory Service migrations..."
	migrate -path $(INVENTORY_SERVICE)/migrations -database "postgres://orderhub:orderhub_secret@localhost:5433/inventory?sslmode=disable" up

migrate-payment:
	@echo "Running Payment Service migrations..."
	migrate -path $(PAYMENT_SERVICE)/migrations -database "postgres://orderhub:orderhub_secret@localhost:5434/payments?sslmode=disable" up

# Create migration files
migration-create-order:
	@echo "Creating Order Service migration..."
	migrate create -ext sql -dir $(ORDER_SERVICE)/migrations -seq $(name)

migration-create-inventory:
	@echo "Creating Inventory Service migration..."
	migrate create -ext sql -dir $(INVENTORY_SERVICE)/migrations -seq $(name)

migration-create-payment:
	@echo "Creating Payment Service migration..."
	migrate create -ext sql -dir $(PAYMENT_SERVICE)/migrations -seq $(name)

# Run services locally
run-order:
	@echo "Running Order Service..."
	cd $(ORDER_SERVICE) && $(GOCMD) run ./...

run-inventory:
	@echo "Running Inventory Service..."
	cd $(INVENTORY_SERVICE) && $(GOCMD) run ./...

run-payment:
	@echo "Running Payment Service..."
	cd $(PAYMENT_SERVICE) && $(GOCMD) run ./...

# Docker commands
docker-build:
	@echo "Building Docker images..."
	docker-compose build

docker-up:
	@echo "Starting Docker containers..."
	docker-compose up -d

docker-down:
	@echo "Stopping Docker containers..."
	docker-compose down

docker-logs:
	@echo "Showing Docker logs..."
	docker-compose logs -f

# Kubernetes commands
k8s-apply:
	@echo "Applying Kubernetes manifests..."
	kubectl apply -f k8s/

k8s-delete:
	@echo "Deleting Kubernetes resources..."
	kubectl delete -f k8s/

k8s-lint:
	@echo "Linting Kubernetes manifests..."
	kubeval k8s/

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	find . -type f -name "*.pb.go" -delete

# Lint code
lint:
	@echo "Linting code..."
	golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# Install tools
install-tools:
	@echo "Installing development tools..."
	$(GOGET) github.com/golangci/golangci-lint/cmd/golangci-lint
	$(GOGET) google.golang.org/protobuf/cmd/protoc-gen-go
	$(GOGET) google.golang.org/grpc/cmd/protoc-gen-go-grpc

# Help
help:
	@echo "OrderHub Makefile Commands:"
	@echo ""
	@echo "  build          - Build all services"
	@echo "  test           - Run all tests"
	@echo "  proto          - Generate protobuf files"
	@echo "  deps           - Download dependencies"
	@echo "  tidy           - Tidy go.mod files"
	@echo "  migrate        - Run all migrations"
	@echo "  docker-build   - Build Docker images"
	@echo "  docker-up      - Start Docker containers"
	@echo "  docker-down    - Stop Docker containers"
	@echo "  k8s-apply      - Apply Kubernetes manifests"
	@echo "  clean          - Clean build artifacts"
	@echo "  lint           - Lint code"
	@echo "  fmt            - Format code"
	@echo "  help           - Show this help message"
