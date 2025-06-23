"""Pytest configuration and fixtures."""

import os
from pathlib import Path

import pytest

from metadata_ingestion.dagster_manager import get_dagster_manager
from metadata_ingestion.db_utils import setup_database


@pytest.fixture(scope="function")
def dagster_manager():
    """Provide a fresh Dagster manager for each test."""
    # Create a new manager with test mode enabled to use ephemeral instance
    import tempfile

    temp_dir = tempfile.mkdtemp(prefix="test_dagster_")
    manager = get_dagster_manager({"temp_dir": temp_dir, "test_mode": True})

    yield manager

    # Cleanup after test
    manager.cleanup()


@pytest.fixture(scope="function")
def test_duckdb():
    """Create a temporary DuckDB database for testing using the shared utility."""
    db_path = setup_database()
    yield db_path

    # Cleanup
    try:
        os.remove(db_path)
    except OSError:
        pass  # Ignore cleanup errors if file doesn't exist


@pytest.fixture(scope="session")
def pipeline_configs():
    """Provide paths to pipeline configuration files."""
    return {"api": Path("pipelines/placeholder_api.json"), "duckdb": Path("pipelines/duck_db.json")}


@pytest.fixture(scope="function", autouse=True)
def output_cleanup():
    """Clean up output directories before and after tests."""
    # Clean up before test
    import shutil

    output_dir = Path("output")
    if output_dir.exists():
        shutil.rmtree(output_dir)

    yield

    # Clean up after test
    if output_dir.exists():
        shutil.rmtree(output_dir)
