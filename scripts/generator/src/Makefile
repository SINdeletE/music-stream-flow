PYTHON=python3.14
PIP=pip

.PHONY: run setup clean

setup: requirements.txt
	${PYTHON} -m venv .venv
	./.venv/bin/${PIP} install --upgrade ${PIP}
	./.venv/bin/${PIP} install -r requirements.txt

run: main.py
	./.venv/bin/${PYTHON} ./main.py

clean:
	find . -path './.venv' -prune -o -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -path './.venv' -prune -o -type f \( -name '*.pyc' -o -name '*.pyo' \) -exec rm -f {} +
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
