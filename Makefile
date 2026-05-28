# Makefile for YuniKorn Exporter

# Variables
IMAGE_NAME ?= yunikorn-exporter
IMAGE_TAG ?= latest
REGISTRY ?= your-registry
NAMESPACE ?= yunikorn-system

.PHONY: help build push deploy undeploy clean test

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build Docker image
	docker build -t $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "Built image: $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)"

build-local: ## Build local Go binary
	go build -o yunikorn-exporter exporter.go

push: ## Push Docker image to registry
	docker push $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	@echo "Pushed image: $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)"

deploy: ## Deploy to Kubernetes
	kubectl apply -k k8s
	@echo "Deployed to namespace: $(NAMESPACE)"

undeploy: ## Remove from Kubernetes
	kubectl delete -k k8s
	@echo "Removed from namespace: $(NAMESPACE)"

logs: ## Show exporter logs
	kubectl logs -n $(NAMESPACE) -l app=yunikorn-exporter -f

port-forward: ## Port forward to local machine
	kubectl port-forward -n $(NAMESPACE) svc/yunikorn-exporter 9300:9300

test: ## Run tests
	go test -v ./...

clean: ## Clean build artifacts
	rm -f yunikorn-exporter
	docker rmi $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG) 2>/dev/null || true

format: ## Format Go code
	go fmt ./...

lint: ## Run linter
	golangci-lint run

deps: ## Download dependencies
	go mod download

verify: ## Verify dependencies
	go mod verify
