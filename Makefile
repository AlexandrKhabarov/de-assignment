SHELL:=/usr/bin/env bash

.PHONY: lint
lint:
	poetry run mypy de_assignment
	poetry run flake8 de_assignment tests

.PHONY: unit
unit:
	poetry run pytest tests/unit

.PHONY: unit
unit:
	poetry run pytest tests/integration

.PHONY: package
package:
	poetry run poetry check
	poetry run pip check

.PHONY: build
build:
	poetry build -f wheel

.PHONY: test
test: lint unit package