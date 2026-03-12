.PHONY : check format clean init
.SILENT: clean init

clean:
	rm -rf *.egg-info; \
	rm -rf dist; \
	rm -rf **/__pycache__; \

init:
	pip install .[rest]

dev:
	pip install .[rest,dev]

build-package:
	python -m build

check:
	python -m ruff check pg_anon
	python -m mypy pg_anon

.PHONY: fix
fix: fix/style fix/fmt

.PHONY: fix/style
fix/style:
	@echo "~~> Fixing linter errors"
	@python -m ruff check --fix

.PHONY: fix/fmt
fix/fmt:
	@echo "~~> Fixing formatter errors"
	@python -m ruff format
