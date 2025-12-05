# Contributing to VecLite

Thank you for your interest in contributing to VecLite!

## Development Setup

1. Clone the repository
2. Install Go 1.21 or later
3. Run `make deps` to install dependencies
4. Run `make test` to verify everything works

## Code Style

- Follow standard Go formatting (`go fmt`)
- Write tests for new features
- Add documentation for public APIs
- Keep functions focused and small

## Project Structure

- `pkg/veclite/` - Public API (what users import)
- `internal/` - Private implementation details
- `cmd/` - Example applications
- Tests should be in `*_test.go` files alongside the code

## Making Changes

1. Create a feature branch
2. Make your changes
3. Add tests
4. Run `make test` and `make lint`
5. Submit a pull request

