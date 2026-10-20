PYTHON ?= python3
VENV := .venv
VENV_PYTHON := $(VENV)/bin/python
COMPOSE = docker compose --env-file "$(CURDIR)/.env" -f "$(CURDIR)/docker-compose.yaml"

.PHONY: setup test test-unit test-integration lint lint-fix lint-format dag check up down

setup:
	$(PYTHON) -m venv "$(VENV)"
	"$(VENV_PYTHON)" -m pip install --upgrade pip
	"$(VENV_PYTHON)" -m pip install -r ./pipelines/requirements.txt
	"$(VENV_PYTHON)" -m pip install -r ./pipelines/requirements-test.txt

test:
	@test -x "$(VENV_PYTHON)" || { echo "Сначала выполните make setup"; exit 1; }
	@echo "Unit tests"
	@PYTHONPATH="$(CURDIR)/pipelines" "$(VENV_PYTHON)" -m pytest pipelines/tests/unit -v
	@echo "Integration tests"
	@PYTHONPATH="$(CURDIR)/pipelines" "$(VENV_PYTHON)" -m pytest pipelines/tests/integration -v

test-unit:
	@test -x "$(VENV_PYTHON)" || { echo "Сначала выполните make setup"; exit 1; }
	@echo "Unit tests"
	@PYTHONPATH="$(CURDIR)/pipelines" "$(VENV_PYTHON)" -m pytest pipelines/tests/unit -v

test-integration:
	@test -x "$(VENV_PYTHON)" || { echo "Сначала выполните make setup"; exit 1; }
	@echo "Integration tests"
	@PYTHONPATH="$(CURDIR)/pipelines" "$(VENV_PYTHON)" -m pytest pipelines/tests/integration -v

lint:
	"$(VENV_PYTHON)" -m ruff check pipelines
	"$(VENV_PYTHON)" -m ruff format --check pipelines

lint-fix:
	"$(VENV_PYTHON)" -m ruff check --fix pipelines

lint-format:
	"$(VENV_PYTHON)" -m ruff format pipelines

dag:
	sh ./dag.sh

check:
	$(COMPOSE) config --quiet

up: dag check
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down
