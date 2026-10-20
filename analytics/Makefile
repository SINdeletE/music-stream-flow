PYTHON ?= python3
VENV := .venv
VENV_PYTHON := $(VENV)/bin/python

.PHONY: setup test lint

setup:
	$(PYTHON) -m venv "$(VENV)"
	"$(VENV_PYTHON)" -m pip install --upgrade pip
	"$(VENV_PYTHON)" -m pip install -r requirements-test.txt

test:
	@test -x "$(VENV_PYTHON)" || { echo "Сначала выполните make setup"; exit 1; }
	"$(VENV_PYTHON)" -m pytest tests -v

lint:
	"$(VENV_PYTHON)" -m ruff check tests
	"$(VENV_PYTHON)" -m ruff format --check tests
