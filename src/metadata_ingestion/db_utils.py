"""Database utility functions."""

import os
import tempfile

import duckdb

from metadata_ingestion import logger

DB_PATH = os.path.join(tempfile.gettempdir(), "ingestion.duckdb")


def setup_database(db_path: str = DB_PATH) -> str:
    """Create a DuckDB database with a predefined schema and test data.

    This function is idempotent; it won't fail if the table already exists.
    It's designed to be called at the start of the application or tests.

    Args:
        db_path: The file path to the DuckDB database.

    Returns:
        The path to the database file.
    """
    logger.info(f"Setting up DuckDB database at: {db_path}")

    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    try:
        conn = duckdb.connect(db_path)

        # Using CREATE TABLE IF NOT EXISTS makes this function safe to call multiple times.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
        """)

        # For simplicity, we'll clear the table and insert fresh data on each setup.
        # This emulates the fresh state provided by the original fixture.
        conn.execute("DELETE FROM test_table")

        conn.execute("""
            INSERT INTO test_table VALUES
            (1, 'Alice', 100.5),
            (2, 'Bob', 200.7),
            (3, 'Charlie', 300.9)
        """)
        conn.close()
        logger.info("Database setup complete. 'test_table' is ready.")
    except Exception as e:
        logger.error(f"Failed to set up DuckDB database: {e}")
        raise

    return db_path


def get_db_path() -> str:
    """Returns the configured path for the DuckDB database."""
    return DB_PATH
