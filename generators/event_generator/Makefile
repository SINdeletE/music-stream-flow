PYTHON ?= python3.14
VENV := .venv
PY := $(VENV)/bin/python

.PHONY: setup run test test-unit test-integration lint clean

setup:
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install -r requirements.txt -r requirements-test.txt

run:
	PYTHONPATH=../.. $(PY) -m generators.event_generator

test:
	PYTHONPATH=../.. $(PY) -m pytest tests -v

test-unit:
	PYTHONPATH=../.. $(PY) -m pytest tests/unit -v

test-integration:
	PYTHONPATH=../.. $(PY) -m pytest tests/integration -v

lint:
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

clean:
	find . -path './.venv' -prune -o -type d -name '__pycache__' -prune -exec rm -r {} +
	find . -path './.venv' -prune -o -type f \( -name '*.pyc' -o -name '*.pyo' \) -exec rm {} +
