# ==============================================================================
# MailBaby Makefile
# ==============================================================================

BINARY_NAME   := mailbaby
BUILD_DIR     := build/bin
CONFIG_FILE   := config.yaml
VERSION       ?= 1.0.0
COMMIT        ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS       := -s -w \
                 -X mailbaby/internal/cmd.Version=$(VERSION) \
                 -X mailbaby/internal/cmd.Commit=$(COMMIT) \
                 -X mailbaby/internal/cmd.BuildDate=$(BUILD_DATE)

.PHONY: all build run check test proto clean docker help

all: build

## proto: Generate protobuf and gRPC Go stubs from proto/mailbaby.proto
proto:
	@echo "==> Generating protobuf stubs..."
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/mailbaby.proto
	@echo "==> Protobuf code generation complete."

## build: Compile the binary to build/bin/mailbaby
build: proto
	@mkdir -p $(BUILD_DIR)
	@echo "==> Compiling $(BINARY_NAME) (version: $(VERSION), commit: $(COMMIT))..."
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) .
	@echo "==> Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

## run: Run the email daemon using the compiled binary
run: build
	@echo "==> Starting $(BINARY_NAME) server..."
	./$(BUILD_DIR)/$(BINARY_NAME) server -c $(CONFIG_FILE)

## check: Validate configuration and test connectivity
check: build
	@echo "==> Running configuration and connectivity check..."
	./$(BUILD_DIR)/$(BINARY_NAME) check -c $(CONFIG_FILE)

## test: Run all automated unit and integration test suites
test:
	@echo "==> Running all test suites..."
	go test -v ./...

## clean: Remove compiled binary artifacts
clean:
	@echo "==> Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@echo "==> Clean complete."

## docker: Build production Docker container image
docker:
	@echo "==> Building Docker image mailbaby:$(VERSION)..."
	docker build -t mailbaby:$(VERSION) -t mailbaby:latest -f build/Dockerfile .

## help: Display available Makefile targets
help:
	@echo "MailBaby Build Management"
	@echo
	@echo "Usage:"
	@echo "  make <target>"
	@echo
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -e 's/## //g' | awk 'BEGIN {FS = ": "}; {printf "  %-12s %s\n", $$1, $$2}'
