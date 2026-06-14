.DEFAULT_GOAL := help

.PHONY: help install lint lint-sql test ci docker-build docker-test up down fault-demo fault-off

help: ## List available targets
	@grep -E '^[a-zA-Z][a-zA-Z0-9_-]*:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-16s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv
	uv sync

lint: ## Ruff lint and format check
	uv run ruff check .
	uv run ruff format --check .

lint-sql: ## sqlfluff lint — RisingWave/ansi dialect (parse errors suppressed; see .sqlfluff)
	uv run sqlfluff lint sql/

test: ## Run the test suite
	uv run pytest -v

ci: lint lint-sql test ## Run the full CI suite locally (ruff + sqlfluff + pytest, no containers)

docker-build: ## Build the project image
	docker build -t marketplace-streaming .

docker-test: ## Run the test suite inside the image
	docker run --rm marketplace-streaming

up: ## Start the full stack (Phase 1+)
	docker compose up --build

down: ## Stop the full stack
	docker compose down -v

fault-demo: ## Run the fault injection demo sequence (Phase 2+)
	@echo "Enabling late_arrival fault..."
	python scripts/fault_control.py --mode late_arrival
	@echo "Waiting 60 seconds for MVs to reflect..."
	@sleep 60
	@echo "Disabling fault, waiting for convergence..."
	python scripts/fault_control.py --mode off
	@sleep 30
	@echo "Fault demo complete. Check Dagster UI (localhost:3000) for reconciliation results."

fault-off: ## Disable all fault injection
	python scripts/fault_control.py --mode off
