SHELL := /bin/bash
PYTHONPATH := src

RATE ?= 40
DURATION ?= 30
SQL ?= select * from gold_daily_revenue order by order_date desc
EVENTS ?= 3000

.PHONY: help demo up stream consume produce transform query stats down logs test lint format clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

demo: ## Run the full pipeline without Kafka (local demo)
	PYTHONPATH=$(PYTHONPATH) python -m streamlake.cli demo --events $(EVENTS)

up: ## Start Kafka + Kafka UI (http://localhost:8080)
	docker compose up -d kafka kafka-ui

stream: ## Start the streaming consumer in the background
	docker compose up -d processor

consume: ## Run the consumer in the foreground
	docker compose run --rm processor consume --transform-every 2

produce: ## Produce simulated events at RATE events/sec for DURATION seconds
	docker compose run --rm processor produce --rate $(RATE) --duration $(DURATION)

transform: ## Rebuild the silver/gold models
	docker compose run --rm processor transform

query: ## Run SQL against the warehouse (SQL="select ...")
	docker compose run --rm processor query "$(SQL)"

stats: ## Show warehouse table sizes and model run history
	docker compose run --rm processor stats

down: ## Stop the stack
	docker compose down

logs: ## Tail consumer logs
	docker compose logs -f processor

test: ## Run the test suite (no broker required)
	pytest

lint: ## Lint the codebase
	ruff check src tests

format: ## Format + autofix the codebase
	ruff format src tests
	ruff check --fix src tests

clean: ## Remove local lake data, warehouse and caches (DESTRUCTIVE)
	rm -rf lake warehouse .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
