.DEFAULT_GOAL := help

.PHONY: help install lint lint-sql test ci integration test-reconciliation docker-build docker-test up down fault-demo fault-off

help: ## List available targets
	@grep -E '^[a-zA-Z][a-zA-Z0-9_-]*:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-16s %s\n", $$1, $$2}'

install: ## Install dependencies into .venv
	uv sync

lint: ## Ruff lint and format check
	uv run ruff check .
	uv run ruff format --check .

lint-sql: ## sqlfluff lint — RisingWave/ansi dialect (parse errors suppressed; see .sqlfluff)
	uv run sqlfluff lint sql/

test: ## Run fast unit tests (no containers, ~5s)
	uv run pytest -v -m "not integration"

ci: lint lint-sql test ## Run the full fast CI suite locally (ruff + sqlfluff + pytest, no containers)

integration: ## Run integration tests (requires Docker; boots the docker-compose topology)
	uv sync --group integration
	uv pip install -e .
	uv run pytest tests/integration/ -v --timeout=300

test-reconciliation: ## Run only the batch-vs-stream reconciliation integration test (Docker)
	uv sync --group integration
	uv pip install -e .
	uv run pytest tests/integration/test_reconciliation.py -v --timeout=300

docker-build: ## Build the project image
	docker build -t marketplace-streaming .

docker-test: ## Run the test suite inside the image
	docker run --rm marketplace-streaming

up: ## Start the full stack (Phase 1+)
	docker compose up --build

down: ## Stop the full stack
	docker compose down -v

fault-demo: ## Run fault injection demo: switch to 6h watermark, enable faults, wait, restore
	@echo "Switching to fault-injection watermark mode (6 hours)..."
	uv run python scripts/switch_watermark.py --mode fault
	@echo "Enabling late_arrival fault injection..."
	uv run python scripts/fault_control.py --mode late_arrival
	@echo "Waiting 60 seconds for MVs to reflect the fault..."
	@sleep 60
	@echo "Disabling fault injection..."
	uv run python scripts/fault_control.py --mode off
	@echo "Waiting 30 seconds for convergence..."
	@sleep 30
	@echo "Restoring standard watermark mode (5 minutes)..."
	uv run python scripts/switch_watermark.py --mode standard
	@echo "Fault demo complete. Check Dagster UI (localhost:3000) for reconciliation results."

fault-off: ## Disable all fault injection
	uv run python scripts/fault_control.py --mode off
