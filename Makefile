# Go parameters
GOCMD = go
GOBUILD = $(GOCMD) build
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test
GOGET = $(GOCMD) get
BINARY_NAME = sensor-event-ingest
VERSION?=2.0
TARGET_PLATFORMS=linux/amd64 linux/arm64
PROTOC = protoc
PROJECT_PATH = ./cmd
COMMIT_HASH = $(shell git rev-parse --short HEAD)
LICENSE = MIT

# Docker parameters
DOCKER_IMAGE_NAME = mfscy/snort3-stream
DOCKER_IMAGE_TAG = 2

comma:= ,
empty:=
space:= $(empty) $(empty)
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

build: go-tidy build-linux ## Build the project and put the output binary in out/bin/

build-linux: ## Build for linux platform
	@$(foreach platform, $(TARGET_PLATFORMS), \
		echo "[INFO] Compiling for $(platform)"; \
		GOOS=$(word 1,$(subst /, ,$(platform))) GOARCH=$(word 2,$(subst /, ,$(platform))) GO111MODULE=on CGO_ENABLED=1 $(GOCMD) build \
			-ldflags "-X main.appVersion=${VERSION} -X main.appCommit=${COMMIT_HASH} -X main.appLicense=${LICENSE}" -tags dynamic	\
			-o out/bin/$(BINARY_NAME)-$(word 1,$(subst /, ,$(platform)))-$(word 2,$(subst /, ,$(platform))) $(PROJECT_PATH) ;\
	)

clean: ## Remove build related file
	@rm -f ./out/bin/mesentinel-*
	@echo "[INFO] Any build output removed."

go-tidy: ## tidy go mod
	@$(GOCMD) mod tidy

test: ## Run tests
	$(GOTEST) -v ./...

run: ## Run with go run
	$(GOBUILD) -o $(BINARY_NAME) -v $(PROJECT_PATH)
	./$(BINARY_NAME)

build-docker: ## Build Docker Image locally
	docker build -t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) .

run-docker: ## Run Docker Image locally
	docker run --rm -it $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)

proto-compile: ## Compile proto file
	$(PROTOC) --go_out=./internal protos/sensor_event.proto

help: ## Show this help.
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${CYAN}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)

.PHONY: build clean test run docker-build proto-compile