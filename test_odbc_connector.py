#!/usr/bin/env python3
"""
Test script for DuckDB connectivity using ODBC module.

This script demonstrates how to test DuckDB connectivity using the existing
ODBC connector infrastructure.
"""

import os
import tempfile
from pathlib import Path

from metadata_ingestion import logger
from metadata_ingestion.config.models import Pipeline
from metadata_ingestion.connectors.odbc_connector import Odbc
from metadata_ingestion.dagster_manager import get_dagster_manager


def create_test_duckdb() -> str:
    """Create a temporary DuckDB database for testing."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.duckdb")

    # Create a simple DuckDB database with test data
    import duckdb

    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            name VARCHAR,
            value DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO test_table VALUES
        (1, 'Alice', 100.5),
        (2, 'Bob', 200.7),
        (3, 'Charlie', 300.9)
    """)
    conn.close()

    logger.info(f"Created test DuckDB at: {db_path}")
    return db_path


def test_duckdb_table_odbc_connectivity():
    """Test DuckDB connectivity using ODBC connector."""
    dagster_manager = get_dagster_manager()
    # Create test database
    db_path = create_test_duckdb()

    try:
        # Load pipeline configuration and get the first source
        pipeline = Pipeline.from_json(Path("pipelines/duck_db.json"))
        source = pipeline.sources[0]  # No query

        # Dynamically update the connection string with the correct db_path
        source.connection["odbc_connection_string"] = f"DRIVER={{DuckDB Driver}};Database={db_path}"

        # Create ODBC connector, which will trigger the Dagster job
        Odbc(source)

        run_status = dagster_manager.get_run_status()

        job_run = next((run for run in run_status if run["job_name"] == f"{source.name}_job"), None)

        assert job_run, "DuckDB job run not found"
        assert job_run["status"] == "SUCCESS", f"DuckDB job failed with status {job_run['status']}"
        logger.info("✅ DuckDB job test PASSED")

    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")

    finally:
        # Cleanup
        try:
            os.remove(db_path)
            os.rmdir(os.path.dirname(db_path))
            logger.info("Cleaned up test database")
        except Exception as e:
            logger.warning(f"Failed to cleanup: {e}")


def test_duckdb_query_odbc_connectivity():
    """Test ODBC connector when no query is provided."""
    dagster_manager = get_dagster_manager()
    # Create test database
    db_path = create_test_duckdb()

    try:
        # Load pipeline configuration and get the first source
        pipeline = Pipeline.from_json(Path("pipelines/duck_db.json"))
        source = pipeline.sources[1]  # With query

        # Dynamically update the connection string with the correct db_path
        source.connection["odbc_connection_string"] = f"DRIVER={{DuckDB Driver}};Database={db_path}"

        # Create ODBC connector, which will trigger the Dagster job
        Odbc(source)

        run_status = dagster_manager.get_run_status()

        job_run = next((run for run in run_status if run["job_name"] == f"{source.name}_job"), None)

        assert job_run, "DuckDB job run not found"
        assert job_run["status"] == "SUCCESS", f"DuckDB job failed with status {job_run['status']}"
        logger.info("✅ DuckDB job test PASSED")

    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")

    finally:
        # Cleanup
        try:
            os.remove(db_path)
            os.rmdir(os.path.dirname(db_path))
            logger.info("Cleaned up test database")
        except Exception as e:
            logger.warning(f"Failed to cleanup: {e}")


def main():
    """Main test function."""
    logger.info("=== DuckDB ODBC Connectivity Test ===")

    # Test ODBC connector without query
    logger.info("\n--- Testing ODBC Connector without Query ---")
    test_duckdb_query_odbc_connectivity()

    # Test using ODBC connector
    logger.info("\n--- Testing with ODBC Connector ---")
    test_duckdb_table_odbc_connectivity()

    logger.info("\n=== Test Complete ===")


if __name__ == "__main__":
    main()
