# ==================================================================================== #
# HELPERS
# ==================================================================================== #
## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

# ==================================================================================== #
# QUALITY CONTROL
# ==================================================================================== #
## audit: tidy dependencies and format, vet and test all code
.PHONY: audit
audit:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy
	@echo 'Formatting code...'
	go fmt ./...
	@echo 'Vetting code...'
	go vet ./...
	staticcheck ./...
	@echo 'Running tests...'
	go clean -testcache && go test -race -vet=off ./...

# ==================================================================================== #
# BUILD
# ==================================================================================== #
current_time = $(shell date --iso-8601=seconds)
git_description = $(shell git describe --always --dirty --tags --long)
linker_flags = "-s -X 'kcli/cmd.buildTime=${buildTime}' -X 'kcli/cmd.version=${git_description}'"

## build: build the cmd application
.PHONY: build
build:
	@echo 'Building cmd...'
	go build -ldflags=${linker_flags} -o=./bin/kcli .
	GOOS=linux GOARCH=amd64 go build -ldflags=${linker_flags} -o=./bin/linux_amd64/kcli .