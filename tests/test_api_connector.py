"""Tests for API connector functionality."""

import json

import pytest  # noqa: F401

from metadata_ingestion.config.models import Pipeline
from metadata_ingestion.connectors.api_connector import Api


class TestApiConnector:
    """Test class for API connector."""

    def test_api_connector_dagster_jobs(self, dagster_manager, pipeline_configs):
        """Test the full Dagster job execution for the Api connector."""
        # Load pipeline configuration and get the sources
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source_get = pipeline.sources[0]
        source_post = pipeline.sources[1]

        # Test GET request job
        Api(source_get)  # This will trigger the Dagster job

        # Test POST request job
        Api(source_post)  # This will trigger the Dagster job

        run_status = dagster_manager.get_run_status()

        # Check that two jobs were executed successfully
        assert len(run_status) >= 2, f"Expected at least 2 runs, but found {len(run_status)}"

        get_job_run = next(
            (run for run in run_status if run["job_name"] == "placeholder_API_GET_job"), None
        )
        post_job_run = next(
            (run for run in run_status if run["job_name"] == "placeholder_API_POST_job"), None
        )

        assert get_job_run, "GET job run not found"
        assert get_job_run["status"] == "SUCCESS", (
            f"GET job failed with status {get_job_run['status']}"
        )

        assert post_job_run, "POST job run not found"
        assert post_job_run["status"] == "SUCCESS", (
            f"POST job failed with status {post_job_run['status']}"
        )

    def test_api_connector_write_raw(self, pipeline_configs):
        """Test the write_raw method of API connector."""
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source = pipeline.sources[0]

        connector = Api(source)

        # Test data
        test_data = {"test": "data", "number": 123}

        # Write raw data
        connector.write_raw(test_data)

        # Check if file was created
        output_dir = connector.output_base / "raw" / source.name
        assert output_dir.exists(), "Raw output directory should exist"

        json_files = list(output_dir.glob("*.json"))
        assert len(json_files) == 1, "Should have created one JSON file"

        # Verify content
        with open(json_files[0]) as f:
            saved_data = json.load(f)
        assert saved_data == test_data, "Saved data should match input data"

    def test_api_connector_write_delta(self, pipeline_configs):
        """Test the write_delta method of API connector."""
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source = pipeline.sources[0]

        connector = Api(source)

        # Test data - list of dictionaries
        test_data = [
            {"id": 1, "name": "Alice", "value": 100.5},
            {"id": 2, "name": "Bob", "value": 200.7},
        ]

        # Write delta data
        connector.write_delta(test_data)

        # Check if delta directory was created
        output_dir = connector.output_base / "delta" / source.name
        assert output_dir.exists(), "Delta output directory should exist"

        # Check for delta files (should contain _delta_log directory)
        delta_log_dir = output_dir / "_delta_log"
        assert delta_log_dir.exists(), "Delta log directory should exist"

    def test_api_connector_write_delta_single_dict(self, pipeline_configs):
        """Test write_delta with a single dictionary."""
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source = pipeline.sources[0]

        connector = Api(source)

        # Test data - single dictionary
        test_data = {"id": 1, "name": "Alice", "value": 100.5}

        # Write delta data
        connector.write_delta(test_data)

        # Check if delta directory was created
        output_dir = connector.output_base / "delta" / source.name
        assert output_dir.exists(), "Delta output directory should exist"

    def test_api_connector_write_methods_empty_data(self, pipeline_configs):
        """Test write methods with empty data."""
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source = pipeline.sources[0]

        connector = Api(source)

        # Test with None data
        connector.write_raw(None)
        connector.write_delta(None)

        # Test with empty list
        connector.write_raw([])
        connector.write_delta([])

        # Should not create any files
        output_dir = connector.output_base
        if output_dir.exists():
            raw_files = (
                list((output_dir / "raw").rglob("*.json")) if (output_dir / "raw").exists() else []
            )
            delta_dirs = (
                list((output_dir / "delta").rglob("_delta_log"))
                if (output_dir / "delta").exists()
                else []
            )

            assert len(raw_files) == 0, "Should not create raw files for empty data"
            assert len(delta_dirs) == 0, "Should not create delta files for empty data"
