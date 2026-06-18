.PHONY: all

ALL_PACKAGES=$(shell go list ./... | grep -v "vendor")
APP_EXECUTABLE="raccoon"
COVER_FILE="/tmp/coverage.out"
GOTEST_TAGS ?= dynamic

all: setup compile

# Setups
setup: copy-config
	make update-deps

update-deps:
	go mod tidy -v
	go mod vendor

copy-config:
	cp .env.sample .env

# Build Lifecycle
compile:
	go build -tags $(GOTEST_TAGS) -o $(APP_EXECUTABLE)

build: copy-config update-deps compile

install:
	go install $(ALL_PACKAGES)

start: build
	./$(APP_EXECUTABLE)

# Utility

fmt:
	go fmt $(ALL_PACKAGES)

vet:
	go vet $(ALL_PACKAGES)

lint:
	@for p in $(ALL_PACKAGES); do \
		echo "==> Linting $$p"; \
		golint $$p | { grep -vwE "exported (var|function|method|type|const) \S+ should have comment" || true; } \
	done

mock:
	@echo "🗑️  Cleaning up old mock directories..."
	@find . -type d -name "mocks" -exec rm -rf {} +
	@echo "⚙️  Regenerating mock files..."
	@mockery
	@echo "✅  Mocks generated successfully!"

# Tests

test: lint
	ENVIRONMENT=test go test -tags $(GOTEST_TAGS) $(shell go list ./... | grep -v "vendor" | grep -v "integration") -v
	@go list ./... | grep -v "vendor" | grep -v "integration" | xargs go test -tags $(GOTEST_TAGS) -count 1 -cover -short -race -timeout 1m -coverprofile ${COVER_FILE}
	@go tool cover -func ${COVER_FILE} | tail -1 | xargs echo test coverage:

coverage:
	ENVIRONMENT=test go test -tags $(GOTEST_TAGS) -coverprofile=${COVER_FILE} $(shell go list ./... | grep -v "vendor" | grep -v "integration") -v
	go tool cover -html=${COVER_FILE}

test-bench: # run benchmark tests
	@go test -tags $(GOTEST_TAGS) $(shell go list ./... | grep -v "vendor") -v -bench ./... -run=^Benchmark

test_ci: setup test

# Docker Run

docker-run:
	docker compose up -d --build

docker-stop:
	docker-compose stop

docker-start:
	docker-compose start

