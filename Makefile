GO    ?= go
RUNS  ?= 100
CHAOS_DURATION ?= 10s

.PHONY: build test test-race vet cluster-up cluster-down cluster-status kill-leader bench chaos chaos-negative clean help \
	gke-state-bucket up deploy down gke-status gke-bench gke-chaos

build: ## Build the node binary and the chaos harness into ./bin
	$(GO) build -o bin/quorumdb ./cmd/quorumdb
	$(GO) build -o bin/chaos ./cmd/chaos

test: ## Unit and integration tests
	$(GO) test ./...

test-race: ## Tests with the race detector
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

cluster-up: ## Start the 3-node cluster in Docker and wait for a leader
	@echo "building image (the first build takes about a minute)..."
	@docker compose build --quiet n1 n2 n3
	docker compose up -d n1 n2 n3
	@scripts/wait-leader.sh

cluster-down: ## Stop the cluster and delete its volumes
	docker compose down -v

cluster-status: ## Show each node's role, term and log position
	@scripts/cluster-status.sh

kill-leader: ## Kill the current leader container to watch a failover
	@leader=$$(scripts/leader.sh) && docker compose kill $$leader >/dev/null && echo "killed $$leader (the leader); restart it with: docker compose start"

# Saved with the results to say where they were measured, e.g. "MacBookPro18,1 Apple M1 Pro".
HOST_DESCRIPTION ?= $(shell sysctl -n hw.model machdep.cpu.brand_string 2>/dev/null | paste -sd' ' -), Docker Desktop

bench: ## redis-benchmark against the leader and a follower, 3 runs each; saves results/local/benchmark.json
	@mkdir -p results/local
	@docker compose build --quiet tools
	docker compose run --rm tools benchmark -environment local -description "$(HOST_DESCRIPTION)" \
		-out /results/benchmark.json $(BENCH_FLAGS)

chaos: build ## Fault-injection test with linearizability checking (env: RUNS CHAOS_DURATION); saves results/local/chaos.json
	@mkdir -p results/local
	bin/chaos -runs $(RUNS) -duration $(CHAOS_DURATION) -summary results/local/chaos.json

chaos-negative: build ## Show that the chaos checker catches deliberately unsafe builds
	-bin/chaos -runs 10 -duration $(CHAOS_DURATION) -node-flags "-unsafe-local-reads" -out chaos-results/unsafe-local-reads
	-bin/chaos -runs 10 -duration $(CHAOS_DURATION) -node-flags "-unsafe-early-ack" -out chaos-results/unsafe-early-ack

clean:
	rm -rf bin chaos-results

# --- GKE (see "Deploy to GKE" in the README). These targets create and delete billed resources. ---

PROJECT_ID      ?= $(shell gcloud config get-value project 2>/dev/null)
REGION          ?= us-west1
TF_STATE_BUCKET ?= $(PROJECT_ID)-quorumdb-tfstate
TERRAFORM       := terraform -chdir=infra/terraform
TERRAFORM_VARS   = -var project_id=$(PROJECT_ID) -var region=$(REGION)
# The scripts read the same project and region from the environment.
GKE_ENV          = PROJECT_ID=$(PROJECT_ID) REGION=$(REGION)

gke-state-bucket: ## One time: create the GCS bucket that holds the Terraform state
	gcloud storage buckets create gs://$(TF_STATE_BUCKET) --project $(PROJECT_ID) --location $(REGION) --uniform-bucket-level-access
	gcloud storage buckets update gs://$(TF_STATE_BUCKET) --versioning

up: ## Create the GKE cluster with Terraform (asks before applying), then deploy QuorumDB
	$(TERRAFORM) init -input=false -backend-config=bucket=$(TF_STATE_BUCKET)
	$(TERRAFORM) apply $(TERRAFORM_VARS)
	gcloud container clusters get-credentials quorumdb --region $(REGION) --project $(PROJECT_ID)
	$(MAKE) deploy

deploy: ## Build and push the image, then apply the Kubernetes manifests and wait for the pods
	$(GKE_ENV) scripts/push-image.sh
	$(GKE_ENV) scripts/gke-deploy.sh

gke-status: ## Show each GKE pod's zone, role, term and log position
	@$(GKE_ENV) scripts/gke-status.sh

gke-bench: ## Run the benchmark Job in the GKE cluster (default node settings); saves results/gke/benchmark.json
	$(GKE_ENV) scripts/gke-deploy.sh base
	$(GKE_ENV) scripts/gke-run-job.sh benchmark $(BENCH_FLAGS)

gke-chaos: ## Run the chaos suite in the GKE cluster (RUNS, default 100); saves results/gke/chaos.json
	$(GKE_ENV) scripts/gke-deploy.sh chaos-settings
	$(GKE_ENV) scripts/gke-run-job.sh chaos -runs=$(RUNS) -duration=$(CHAOS_DURATION)

down: ## Delete QuorumDB and its disks, then destroy everything Terraform created (asks first)
	$(GKE_ENV) scripts/gke-down.sh
	$(TERRAFORM) init -input=false -backend-config=bucket=$(TF_STATE_BUCKET)
	$(TERRAFORM) destroy $(TERRAFORM_VARS)

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'
