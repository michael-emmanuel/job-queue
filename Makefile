GO ?= go

.PHONY: all fmt vet test race cover bench

all: fmt vet race

fmt:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "gofmt needed" && exit 1)

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race -count=1 ./...

cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./internal/...
	$(GO) tool cover -func=coverage.out | tail -n 1

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/...

.PHONY: build run
build:
	$(GO) build -o bin/broker ./cmd/broker

# Local development server: in-process keys for the quickstart script only.
run: build
	JOBQUEUE_API_KEYS="admin-key-0123456789:ops:admin,prod-key-01234567890:acme:producer,work-key-012345678901:workers:worker" \
	./bin/broker -log-json=false -data-dir ./data

.PHONY: docker compose-env compose-up compose-down
docker:
	docker build -t jobqueue:dev .

# Generates local-only secrets into deploy/.env (mode 600, git-ignored).
compose-env:
	@test -f deploy/.env || (umask 077; r() { head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24; }; \
	  a=ak-$$(r); p=pk-$$(r); w=wk-$$(r); \
	  printf 'JOBQUEUE_CLUSTER_SECRET=cs-%s\nWORK_KEY=%s\nPROD_KEY=%s\nADMIN_KEY=%s\nJOBQUEUE_API_KEYS=%s:ops:admin,%s:acme:producer,%s:workers:worker\n' \
	  $$(r) $$w $$p $$a $$a $$p $$w > deploy/.env; echo wrote deploy/.env)

compose-up: docker compose-env
	docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d

compose-down:
	docker compose -f deploy/docker-compose.yml --env-file deploy/.env down
