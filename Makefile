SHELL := /bin/bash
COMPOSE := docker compose
UV := uv
PY_SRC := orchestration/ingestion/src orchestration/ingestion/tests portal/src portal/tests use_case

.PHONY: help up down build restart logs ps dagster-dev ingest dbt-build dbt-parse dbt-lint dbt-format test test-cov lint format check clean ui-build

help:
	@echo "Open Data Lakehouse commands"
	@echo "  make up          Build and start RustFS, Unity Catalog (+ UI), Spark Thrift and Dagster"
	@echo "  make down        Stop containers and remove volumes"
	@echo "  make ui-build    Rebuild only the Unity Catalog UI (bump UC_UI_VERSION first)"
	@echo "  make dagster-dev Run Dagster locally with uv"
	@echo "  make ingest      Materialize every dynamically generated ingestion asset"
	@echo "  make dbt-build   Build dbt staging, Gold and semantic models"
	@echo "  make dbt-lint    Lint the dbt SQL models with the dbt v2 native linter"
	@echo "  make dbt-format  Rewrite the dbt SQL models to the linted style"
	@echo "  make test        Run the pytest + pytest-mock suite"
	@echo "  make lint        Run isort, black, flake8, ruff, mypy and pylint"
	@echo "  make check       Run everything CI runs: Python + SQL + tests"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v --remove-orphans

build:
	$(COMPOSE) build

# The UI is pinned to an upstream release via the UC_UI_VERSION build arg. Bump
# that value in docker-compose.yml, then re-run this to pick up the new code.
ui-build:
	$(COMPOSE) build unity-catalog-ui

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
	$(UV) run isort --check-only --diff $(PY_SRC)
	$(UV) run black --check --line-length=120 $(PY_SRC)
	$(UV) run flake8 --config=.flake8 $(PY_SRC)
	$(UV) run ruff check $(PY_SRC)
	$(UV) run mypy --config-file=pyproject.toml $(PY_SRC)
	$(UV) run pylint --rcfile=.pylintrc orchestration/ingestion/src portal/src use_case

format:
	$(UV) run isort $(PY_SRC)
	$(UV) run black --line-length=120 $(PY_SRC)

# The exact sequence `.github/workflows/ci.yml` runs, minus the matrix: run it
# before pushing so a red build is a surprise rather than the expected outcome.
check: lint test
	DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true DBT_ENGINE_NO_WARN_SEMANTIC_MANIFEST_VALIDATION=true \
		$(UV) run dbt parse --project-dir=dbt_lakehouse --profiles-dir=dbt_lakehouse
	DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true \
		$(UV) run dbt format --check --project-dir=dbt_lakehouse --profiles-dir=dbt_lakehouse
	DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true \
		$(UV) run dbt lint --project-dir=dbt_lakehouse --profiles-dir=dbt_lakehouse
	$(UV) run dagster definitions validate -w workspace.yaml

clean:
	$(COMPOSE) down -v --remove-orphans
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
