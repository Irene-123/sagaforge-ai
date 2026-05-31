.PHONY: build run-all run-order run-inventory run-payment run-fulfillment run-notification run-ai run-orchestrator run-dashboard migrate help

# ──────────────── Build ────────────────

build:
	@echo "Building all services…"
	go build -o bin/order-service       ./cmd/order-service
	go build -o bin/inventory-service   ./cmd/inventory-service
	go build -o bin/payment-service     ./cmd/payment-service
	go build -o bin/fulfillment-service ./cmd/fulfillment-service
	go build -o bin/notification-service ./cmd/notification-service
	go build -o bin/ai-insights-service ./cmd/ai-insights-service
	go build -o bin/saga-orchestrator   ./cmd/saga-orchestrator
	go build -o bin/dashboard           ./cmd/dashboard
	@echo "✅ All binaries in ./bin/"

# ──────────────── Run individual services ────────────────

run-order:
	go run ./cmd/order-service

run-inventory:
	go run ./cmd/inventory-service

run-payment:
	go run ./cmd/payment-service

run-fulfillment:
	go run ./cmd/fulfillment-service

run-notification:
	go run ./cmd/notification-service

run-ai:
	go run ./cmd/ai-insights-service

run-orchestrator:
	go run ./cmd/saga-orchestrator

run-dashboard:
	go run ./cmd/dashboard

# ──────────────── Run everything (requires overmind or goreman) ────────────────

run-all:
	@echo "Starting all services via Procfile…"
	@which overmind > /dev/null 2>&1 && overmind start || \
	  (which goreman > /dev/null 2>&1 && goreman start || \
	   (echo "Install overmind (brew install overmind) or goreman to run all services." && exit 1))

# ──────────────── Database ────────────────

migrate:
	@echo "Running migrations against Supabase…"
	psql "$$DATABASE_URL" -f migrations/001_init.sql
	@echo "✅ Migration complete"

# ──────────────── Helpers ────────────────

test:
	go test ./...

lint:
	golangci-lint run ./...

help:
	@echo ""
	@echo "SagaForge AI — available targets:"
	@echo ""
	@echo "  make build          Build all service binaries into ./bin/"
	@echo "  make run-all        Start all services (needs overmind or goreman)"
	@echo "  make run-<service>  Start a single service (order, inventory, payment,"
	@echo "                      fulfillment, notification, ai, orchestrator, dashboard)"
	@echo "  make migrate        Run SQL migrations against \$$DATABASE_URL"
	@echo "  make test           Run Go tests"
	@echo "  make lint           Run golangci-lint"
	@echo ""
