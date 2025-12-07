.PHONY: build test clean run example

# Build the library
build:
	go build ./...

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Clean build artifacts
clean:
	go clean ./...
	rm -f coverage.out coverage.html
	rm -rf bin/ dist/ build/

# Run example
example:
	go run ./examples/basic/main.go

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	golangci-lint run ./...

# Install dependencies
deps:
	go mod download
	go mod tidy

