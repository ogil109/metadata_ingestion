# Metadata Ingestion

Metadata ingestion package with factory pattern to ingest data to delta lake from diverse sources.

## Quick Start with Docker

```bash
# Build the image
docker build -t metadata-ingestion .

# Run the application
docker run --rm -v $(pwd)/output:/output metadata-ingestion

# Run tests
docker run --rm metadata-ingestion test
```

## Features

- **Multiple Connector Types**: Support for ODBC and API data sources
- **Dual Output Formats**:
  - Raw JSON files for debugging and backup
  - Delta Lake format for analytics and data processing
- **Dagster Integration**: Automatic job creation and execution
- **Comprehensive Testing**: Full pytest test suite with fixtures and mocking

## Installation

Install the package and its dependencies using uv:

```bash
# Install the package
uv sync

# Install development dependencies
uv sync --group dev
```

## Usage

### Defining Connectors

Connectors are defined as subclasses of [`BaseConnector`](src/metadata_ingestion/connectors/base.py:7). They must be placed in the connectors folder for discoverability reasons.

Each connector must implement:
- [`connect()`](src/metadata_ingestion/connectors/base.py:20): Establish connection to the data source
- [`disconnect()`](src/metadata_ingestion/connectors/base.py:25): Close connection to the data source
- [`fetch_data()`](src/metadata_ingestion/connectors/base.py:30): Fetch data from the source
- [`write_raw()`](src/metadata_ingestion/connectors/base.py:35): Write data in raw JSON format
- [`write_delta()`](src/metadata_ingestion/connectors/base.py:40): Write data in Delta Lake format

### Available Connectors

#### ODBC Connector
The [`Odbc`](src/metadata_ingestion/connectors/odbc_connector.py:9) connector supports any ODBC-compatible database:

```python
from metadata_ingestion.connectors.odbc_connector import Odbc
from metadata_ingestion.config.models import Source

# Configure source
source = Source(
    name="my_database",
    connection={
        "odbc_connection_string": "DRIVER={DuckDB Driver};Database=/path/to/db.duckdb",
        "query": "SELECT * FROM my_table"  # Optional
    }
)

# Create connector and fetch data
connector = Odbc(source)
data, columns = connector.fetch_data()

# Write outputs
connector.write_raw((data, columns))  # Creates JSON file in output/raw/
connector.write_delta((data, columns))  # Creates Delta Lake in output/delta/
```

#### API Connector
The [`Api`](src/metadata_ingestion/connectors/api_connector.py:9) connector supports REST APIs:

```python
from metadata_ingestion.connectors.api_connector import Api

# Configure source
source = Source(
    name="my_api",
    connection={
        "endpoint": "https://api.example.com/data",
        "type": "GET",  # or "POST" (Default is GET)
        "args": {"param1": "value1"}  # Query params for GET
    }
)

# Create connector and fetch data
connector = Api(source)
data = connector.fetch_data()

# Write outputs
connector.write_raw(data)  # Creates JSON file in output/raw/
connector.write_delta(data)  # Creates Delta Lake in output/delta/
```

### Output Structure

The connectors create organized output directories:

```
output/
├── raw/
│   └── {source_name}/
│       └── {source_name}_{timestamp}.json
└── delta/
    └── {source_name}/
        ├── _delta_log/
        └── *.parquet
```

### Defining Pipelines

One pipeline (JSON file in `/pipelines`) can contain multiple logically related sources mapped to two unique sinks (raw hub and delta lake), allowing for grouping of related pipelines of different types.

## Testing

The project includes a pytest test suite:

### Running Tests

```bash
# Run all tests using the convenience script
./run_tests.sh

# Run with coverage
./run_tests.sh --cov=src/metadata_ingestion

# Or use uv directly
uv run pytest tests/ -v
```

### Test Structure

- [`tests/conftest.py`](tests/conftest.py:1): Pytest fixtures and configuration
- [`tests/test_api_connector.py`](tests/test_api_connector.py:1): API connector tests
- [`tests/test_odbc_connector.py`](tests/test_odbc_connector.py:1): ODBC connector tests
- [`tests/test_write_methods.py`](tests/test_write_methods.py:1): Integration tests for write methods

### Test Features

- **Automatic cleanup**: Output directories are cleaned after each test
- **Mock databases**: Temporary DuckDB instances for ODBC testing
- **Dagster integration**: Tests include job execution verification
- **Multiple data types**: Tests cover various input data formats

## Development

### Dependencies

- **Core**: dagster, duckdb, pyodbc, deltalake, pyarrow, requests
- **Development**: pytest, pytest-cov, pytest-mock, ruff, pre-commit

### Code Quality

The project uses ruff for linting and formatting:

```bash
# Run linting
uv run ruff check

# Auto-fix issues
uv run ruff check --fix

# Format code
uv run ruff format
```
