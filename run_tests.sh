#!/bin/bash
# Test runner script for the metadata_ingestion project

set -e  # Exit on any error

echo "Running tests with uv..."

# Check if arguments are provided, otherwise use defaults
if [ $# -eq 0 ]; then
    echo "Running with default arguments: tests/ -v --tb=short"
    uv run python -m pytest tests/ -v --tb=short
else
    echo "Running with custom arguments: $@"
    uv run python -m pytest "$@"
fi

echo "Tests completed!"