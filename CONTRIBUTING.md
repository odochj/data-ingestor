# Contributing

## Running tests

Run the test suite from the repository root with uv:

    uv sync
    uv run pytest -q

Unit tests should run without external services. Integration tests may require the local services described in the project README.
