#!/bin/bash

set -e

echo "Running type checks"
poetry run mypy de_assignment

echo "Running lint checks"
poetry run flake8 de_assignment tests
