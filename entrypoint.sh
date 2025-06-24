#!/bin/sh
set -e

# If the first argument is "test", run pytest.
# Otherwise, execute the main application.
if [ "$1" = "test" ]; then
    # Remove "test" from the arguments and run pytest
    shift
    exec uv run pytest "$@"
else
    # Run the main application
    exec uv run python main.py "$@"
fi