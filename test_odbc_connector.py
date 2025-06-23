#!/usr/bin/env python3
"""
Test script for DuckDB connectivity using ODBC module.

This script demonstrates how to test DuckDB connectivity using the existing
ODBC connector infrastructure.
"""

import os
import tempfile

from metadata_ingestion import logger
from metadata_ingestion.config.models import Source
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


def test_duckdb_odbc_connectivity():
    """Test DuckDB connectivity using ODBC connector."""
    dagster_manager = get_dagster_manager()
    # Create test database
    db_path = create_test_duckdb()

    try:
        # Configure DuckDB ODBC connection
        # Note: This requires DuckDB ODBC driver to be installed
        source_config = Source(
            name="test_duckdb_job",
            src_type="odbc",
            driver="{DuckDB Driver}",  # DuckDB ODBC driver name
            server="",  # Not used for DuckDB
            database=db_path,  # Path to DuckDB file
            username="",  # Not required for DuckDB
            password="",  # Not required for DuckDB
            schedule=None,  # No scheduling for test
            query="SELECT * FROM test_table",  # Query to be executed by the job
        )

        # Create ODBC connector, which will trigger the Dagster job
        Odbc(source_config)

        run_status = dagster_manager.get_run_status()

        job_run = next(
            (run for run in run_status if run["job_name"] == "test_duckdb_job_job"), None
        )

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


def test_duckdb_direct_connection():
    """Test DuckDB connectivity using direct connection string."""

    # Create test database
    db_path = create_test_duckdb()

    try:
        import pyodbc

        # Direct ODBC connection string for DuckDB
        connection_string = f"DRIVER={{DuckDB Driver}};Database={db_path};"

        logger.info("Testing direct DuckDB ODBC connection...")
        logger.info(f"Connection string: {connection_string}")

        # Test direct connection
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Test query
        cursor.execute("SELECT * FROM test_table")
        rows = cursor.fetchall()

        logger.info(f"✅ Direct connection successful, fetched {len(rows)} rows")
        for row in rows:
            logger.info(f"  Row: {row}")

        conn.close()
        logger.info("✅ Direct connection test completed")

    except Exception as e:
        logger.error(f"❌ Direct connection test failed: {e}")
        logger.info("This might indicate DuckDB ODBC driver is not installed")

    finally:
        # Cleanup
        try:
            os.remove(db_path)
            os.rmdir(os.path.dirname(db_path))
        except Exception as e:
            logger.warning(f"Failed to cleanup: {e}")


def check_duckdb_odbc_driver():
    """Check if DuckDB ODBC driver is available."""
    try:
        import pyodbc

        drivers = pyodbc.drivers()

        logger.info("Available ODBC drivers:")
        for driver in drivers:
            logger.info(f"  - {driver}")

        duckdb_drivers = [d for d in drivers if "duck" in d.lower()]

        if duckdb_drivers:
            logger.info(f"✅ Found DuckDB ODBC driver(s): {duckdb_drivers}")
            return True
        else:
            logger.warning("❌ No DuckDB ODBC driver found")
            logger.info("To install DuckDB ODBC driver:")
            logger.info("  1. Download from: https://github.com/duckdb/duckdb/releases")
            logger.info("  2. Or install via package manager")
            return False

    except Exception as e:
        logger.error(f"Failed to check ODBC drivers: {e}")
        return False


def main():
    """Main test function."""
    logger.info("=== DuckDB ODBC Connectivity Test ===")

    # Check if DuckDB ODBC driver is available
    if not check_duckdb_odbc_driver():
        logger.warning("Skipping ODBC tests due to missing driver")
        return

    # Test using ODBC connector
    logger.info("\n--- Testing with ODBC Connector ---")
    test_duckdb_odbc_connectivity()

    # Test direct connection
    logger.info("\n--- Testing Direct ODBC Connection ---")
    test_duckdb_direct_connection()

    logger.info("\n=== Test Complete ===")


if __name__ == "__main__":
    main()
