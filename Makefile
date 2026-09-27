GO    ?= go
RUNS  ?= 100
CHAOS_DURATION ?= 10s

.PHONY: build test test-race vet cluster-up cluster-down cluster-status kill-leader bench chaos chaos-negative clean help

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

bench: ## redis-benchmark against the leader (env: REQUESTS CLIENTS PIPELINE TESTS TARGET)
	@scripts/bench.sh

chaos: build ## Fault-injection test with linearizability checking (env: RUNS CHAOS_DURATION)
	bin/chaos -runs $(RUNS) -duration $(CHAOS_DURATION)

chaos-negative: build ## Show that the chaos checker catches deliberately unsafe builds
	-bin/chaos -runs 10 -duration $(CHAOS_DURATION) -node-flags "-unsafe-local-reads" -out chaos-results/unsafe-local-reads
	-bin/chaos -runs 10 -duration $(CHAOS_DURATION) -node-flags "-unsafe-early-ack" -out chaos-results/unsafe-early-ack

clean:
	rm -rf bin chaos-results

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'
