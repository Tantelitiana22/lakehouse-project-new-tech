SHELL := /bin/bash
COMPOSE := docker compose
UV := uv

.PHONY: help up down build restart logs ps dagster-dev ingest dbt-build dbt-parse dbt-lint dbt-format test test-cov lint format clean

help:
	@echo "Open Data Lakehouse commands"
	@echo "  make up          Build and start RustFS, Unity Catalog, Spark Thrift and Dagster"
	@echo "  make down        Stop containers and remove volumes"
	@echo "  make dagster-dev Run Dagster locally with uv"
	@echo "  make ingest      Materialize every dynamically generated ingestion asset"
	@echo "  make dbt-build   Build dbt staging, Gold and semantic models"
	@echo "  make dbt-lint    Lint the dbt SQL models with the dbt v2 native linter"
	@echo "  make dbt-format  Rewrite the dbt SQL models to the linted style"
	@echo "  make test        Run the pytest + pytest-mock suite"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v --remove-orphans

build:
	$(COMPOSE) build

restart:
	$(COMPOSE) restart

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

dagster-dev:
	$(UV) run dagster dev -m use_case -p 3000

ingest:
	$(COMPOSE) exec dagster-webserver dagster asset materialize -m use_case --select '*'

dbt-parse:
	$(COMPOSE) exec dagster-webserver dbt parse --project-dir /app/dbt_lakehouse --profiles-dir /app/dbt_lakehouse

dbt-build:
	$(COMPOSE) exec dagster-webserver dbt build --project-dir /app/dbt_lakehouse --profiles-dir /app/dbt_lakehouse

dbt-lint:
	$(COMPOSE) exec dagster-webserver dbt lint --project-dir /app/dbt_lakehouse --profiles-dir /app/dbt_lakehouse

dbt-format:
	$(COMPOSE) exec dagster-webserver dbt format --project-dir /app/dbt_lakehouse --profiles-dir /app/dbt_lakehouse

test:
	$(UV) run pytest

test-cov:
	$(UV) run pytest --cov=ndr_lakehouse --cov-report=term-missing

lint:
	$(UV) run ruff check orchestration/ingestion/src orchestration/ingestion/tests

format:
	$(UV) run ruff format orchestration/ingestion/src orchestration/ingestion/tests

clean:
	$(COMPOSE) down -v --remove-orphans
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
