"""Tests for ODBC connector functionality."""

import json

import pytest  # noqa: F401

from metadata_ingestion.config.models import Pipeline
from metadata_ingestion.connectors.odbc_connector import Odbc


class TestOdbcConnector:
    """Test class for ODBC connector."""

    def test_odbc_connector_table_dagster_job(
        self, dagster_manager, test_duckdb, pipeline_configs, output_cleanup
    ):
        """Test DuckDB connectivity using ODBC connector without query."""
        # Load pipeline configuration and get the first source (no query)
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[0]

        # Dynamically update the connection string with the correct db_path
        source.connection["odbc_connection_string"] = (
            f"DRIVER={{DuckDB Driver}};Database={test_duckdb}"
        )

        # Create ODBC connector, which will trigger the Dagster job
        Odbc(source)

        run_status = dagster_manager.get_run_status()

        job_run = next((run for run in run_status if run["job_name"] == f"{source.name}_job"), None)

        assert job_run, "DuckDB job run not found"
        assert job_run["status"] == "SUCCESS", f"DuckDB job failed with status {job_run['status']}"

    def test_odbc_connector_query_dagster_job(
        self, dagster_manager, test_duckdb, pipeline_configs, output_cleanup
    ):
        """Test ODBC connector when query is provided."""
        # Load pipeline configuration and get the second source (with query)
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[1]

        # Dynamically update the connection string with the correct db_path
        source.connection["odbc_connection_string"] = (
            f"DRIVER={{DuckDB Driver}};Database={test_duckdb}"
        )

        # Create ODBC connector, which will trigger the Dagster job
        Odbc(source)

        run_status = dagster_manager.get_run_status()

        job_run = next((run for run in run_status if run["job_name"] == f"{source.name}_job"), None)

        assert job_run, "DuckDB job run not found"
        assert job_run["status"] == "SUCCESS", f"DuckDB job failed with status {job_run['status']}"

    def test_odbc_connector_fetch_data(self, test_duckdb, pipeline_configs):
        """Test the fetch_data method returns data and columns."""
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[0]

        # Update connection string
        source.connection["odbc_connection_string"] = (
            f"DRIVER={{DuckDB Driver}};Database={test_duckdb}"
        )

        connector = Odbc(source)
        result = connector.fetch_data()

        assert result is not None, "fetch_data should return data"
        assert isinstance(result, tuple), "fetch_data should return a tuple"
        assert len(result) == 2, "fetch_data should return (data, columns)"

        data, columns = result
        assert len(data) == 3, "Should have 3 rows of test data"
        assert len(columns) == 3, "Should have 3 columns"
        assert columns == ["id", "name", "value"], "Column names should match"

    def test_odbc_connector_write_raw(self, test_duckdb, pipeline_configs):
        """Test the write_raw method of ODBC connector."""
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[0]

        # Update connection string
        source.connection["odbc_connection_string"] = (
            f"DRIVER={{DuckDB Driver}};Database={test_duckdb}"
        )

        connector = Odbc(source)

        # Fetch real data
        data, columns = connector.fetch_data()

        # Write raw data
        connector.write_raw((data, columns))

        # Check if file was created
        output_dir = connector.output_base / "raw" / source.name
        assert output_dir.exists(), "Raw output directory should exist"

        json_files = list(output_dir.glob("*.json"))
        assert len(json_files) == 1, "Should have created one JSON file"

        # Verify content
        with open(json_files[0]) as f:
            saved_data = json.load(f)

        assert isinstance(saved_data, list), "Saved data should be a list"
        assert len(saved_data) == 3, "Should have 3 records"
        assert all(isinstance(record, dict) for record in saved_data), (
            "Each record should be a dict"
        )

        # Check first record structure
        first_record = saved_data[0]
        assert "id" in first_record, "Should have id column"
        assert "name" in first_record, "Should have name column"
        assert "value" in first_record, "Should have value column"

    def test_odbc_connector_write_delta(self, test_duckdb, pipeline_configs):
        """Test the write_delta method of ODBC connector."""
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[0]

        # Update connection string
        source.connection["odbc_connection_string"] = (
            f"DRIVER={{DuckDB Driver}};Database={test_duckdb}"
        )

        connector = Odbc(source)

        # Fetch real data
        data, columns = connector.fetch_data()

        # Write delta data
        connector.write_delta((data, columns))

        # Check if delta directory was created
        output_dir = connector.output_base / "delta" / source.name
        assert output_dir.exists(), "Delta output directory should exist"

        # Check for delta files (should contain _delta_log directory)
        delta_log_dir = output_dir / "_delta_log"
        assert delta_log_dir.exists(), "Delta log directory should exist"

    def test_odbc_connector_write_raw_no_columns(self, pipeline_configs):
        """Test write_raw with data but no column information."""
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[0]

        connector = Odbc(source)

        # Test data without column info
        test_data = [(1, "Alice", 100.5), (2, "Bob", 200.7)]

        # Write raw data without columns
        connector.write_raw(test_data)

        # Check if file was created
        output_dir = connector.output_base / "raw" / source.name
        assert output_dir.exists(), "Raw output directory should exist"

        json_files = list(output_dir.glob("*.json"))
        assert len(json_files) == 1, "Should have created one JSON file"

        # Verify content - should be list of lists
        with open(json_files[0]) as f:
            saved_data = json.load(f)

        assert isinstance(saved_data, list), "Saved data should be a list"
        assert len(saved_data) == 2, "Should have 2 records"
        assert all(isinstance(record, list) for record in saved_data), (
            "Each record should be a list"
        )

    def test_odbc_connector_write_delta_no_columns(self, pipeline_configs):
        """Test write_delta with data but no column information."""
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[0]

        connector = Odbc(source)

        # Test data without column info
        test_data = [(1, "Alice", 100.5), (2, "Bob", 200.7)]

        # Write delta data without columns
        connector.write_delta(test_data)

        # Check if delta directory was created
        output_dir = connector.output_base / "delta" / source.name
        assert output_dir.exists(), "Delta output directory should exist"

        # Check for delta files
        delta_log_dir = output_dir / "_delta_log"
        assert delta_log_dir.exists(), "Delta log directory should exist"
