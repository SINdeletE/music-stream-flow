PYTHON ?= python3.14
VENV := .venv
VENV_PYTHON := $(VENV)/bin/python
ARGS ?=

.PHONY: setup run dry-run clean

setup: requirements.txt
	$(PYTHON) -m venv $(VENV)
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r requirements.txt

run: main.py seed_generator.py
	$(VENV_PYTHON) main.py $(ARGS)

dry-run: main.py seed_generator.py
	$(VENV_PYTHON) main.py --dry-run $(ARGS)

clean:
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -exec rm -f {} +
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
