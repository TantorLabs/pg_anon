.PHONY: help install install-dev build check fix fix/style fix/fmt test openapi clean
.SILENT: clean

help:
	@echo "Available targets:"
	@echo "  install     Install pg_anon with REST API extras"
	@echo "  install-dev Install in editable mode with dev tools (ruff, mypy, pytest)"
	@echo "  build       Build wheel and sdist into ./dist"
	@echo "  check       Run linters (ruff + mypy) over pg_anon/"
	@echo "  fix         Auto-fix lint issues and format the codebase"
	@echo "  test        Run the pytest suite"
	@echo "  openapi     Regenerate pg_anon/rest_api/openapi.json from the REST app"
	@echo "  clean       Remove build artifacts and tool caches"

install:
	pip install ".[api]"

install-dev:
	pip install -e ".[api,dev]"

build:
	python -m build

check:
	python -m ruff check pg_anon
	python -m mypy pg_anon

fix: fix/style fix/fmt

fix/style:
	@echo "~~> Fixing linter errors"
	python -m ruff check --fix

fix/fmt:
	@echo "~~> Formatting code"
	python -m ruff format

test:
	python -m pytest

openapi:
	python -c 'from pg_anon.rest_api.api import generate_openapi_doc_file; generate_openapi_doc_file()'

clean:
	rm -rf *.egg-info dist build
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf .mypy_cache .ruff_cache .pytest_cache
