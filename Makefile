.RECIPEPREFIX = >

.PHONY: install lint format typecheck test check

install:
> uv sync

lint:
> uv run ruff check .
> uv run ruff format --check .

format:
> uv run ruff format .
> uv run ruff check --fix .

typecheck:
> uv run mypy

# Unit tests; no database or broker needed.
test:
> uv run pytest -m "not integration"

# What CI runs on each Python version (CI also scans git history for secrets).
check: lint typecheck test
