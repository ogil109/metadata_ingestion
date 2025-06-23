"""Integration tests for write_raw and write_delta methods."""

import json
from pathlib import Path

import pytest  # noqa: F401

from metadata_ingestion.config.models import Pipeline
from metadata_ingestion.connectors.api_connector import Api
from metadata_ingestion.connectors.odbc_connector import Odbc


class TestWriteMethods:
    """Test class for write methods integration."""

    def test_api_connector_full_pipeline_with_writes(self, pipeline_configs):
        """Test API connector fetch and write pipeline."""
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source = pipeline.sources[0]  # GET request

        connector = Api(source)

        # Fetch data from API
        data = connector.fetch_data()

        if data:  # Only test writes if we got data
            # Test write_raw
            connector.write_raw(data)

            # Check raw output
            raw_dir = Path("output") / "raw" / source.name
            assert raw_dir.exists(), "Raw output directory should exist"

            raw_files = list(raw_dir.glob("*.json"))
            assert len(raw_files) >= 1, "Should have created at least one raw file"

            # Test write_delta
            connector.write_delta(data)

            # Check delta output
            delta_dir = Path("output") / "delta" / source.name
            assert delta_dir.exists(), "Delta output directory should exist"

            delta_log_dir = delta_dir / "_delta_log"
            assert delta_log_dir.exists(), "Delta log directory should exist"

    def test_odbc_connector_full_pipeline_with_writes(
        self, test_duckdb, pipeline_configs, output_cleanup
    ):
        """Test ODBC connector fetch and write pipeline."""
        pipeline = Pipeline.from_json(pipeline_configs["duckdb"])
        source = pipeline.sources[0]

        # Update connection string
        source.connection["odbc_connection_string"] = (
            f"DRIVER={{DuckDB Driver}};Database={test_duckdb}"
        )

        connector = Odbc(source)

        # Fetch data from database
        result = connector.fetch_data()

        if result and result[0]:  # Only test writes if we got data
            # Test write_raw
            connector.write_raw(result)

            # Check raw output
            raw_dir = Path("output") / "raw" / source.name
            assert raw_dir.exists(), "Raw output directory should exist"

            raw_files = list(raw_dir.glob("*.json"))
            assert len(raw_files) >= 1, "Should have created at least one raw file"

            # Verify raw file content
            with open(raw_files[0]) as f:
                raw_data = json.load(f)
            assert isinstance(raw_data, list), "Raw data should be a list"
            assert len(raw_data) == 3, "Should have 3 records"

            # Test write_delta
            connector.write_delta(result)

            # Check delta output
            delta_dir = Path("output") / "delta" / source.name
            assert delta_dir.exists(), "Delta output directory should exist"

            delta_log_dir = delta_dir / "_delta_log"
            assert delta_log_dir.exists(), "Delta log directory should exist"

    def test_write_methods_create_proper_directory_structure(
        self, pipeline_configs, output_cleanup
    ):
        """Test that write methods create proper directory structure."""
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source = pipeline.sources[0]

        connector = Api(source)

        # Test data
        test_data = {"test": "data"}

        # Write raw and delta
        connector.write_raw(test_data)
        connector.write_delta(test_data)

        # Check directory structure
        output_dir = Path("output")
        assert output_dir.exists(), "Output directory should exist"

        raw_dir = output_dir / "raw"
        delta_dir = output_dir / "delta"

        assert raw_dir.exists(), "Raw directory should exist"
        assert delta_dir.exists(), "Delta directory should exist"

        source_raw_dir = raw_dir / source.name
        source_delta_dir = delta_dir / source.name

        assert source_raw_dir.exists(), "Source-specific raw directory should exist"
        assert source_delta_dir.exists(), "Source-specific delta directory should exist"

    def test_write_methods_handle_different_data_types(self, pipeline_configs):
        """Test write methods with different data types."""
        pipeline = Pipeline.from_json(pipeline_configs["api"])
        source = pipeline.sources[0]

        connector = Api(source)

        # Test different data types
        test_cases = [
            {"single": "dict"},
            [{"list": "of"}, {"dicts": "here"}],
            ["simple", "list", "of", "strings"],
            42,  # single number
            "simple string",
        ]

        for i, test_data in enumerate(test_cases):
            # Create a new source for each test to avoid conflicts
            from metadata_ingestion.config.models import Source

            test_source = Source(
                name=f"{source.name}_test_{i}",
                src_type=source.src_type,
                connection=source.connection.copy(),
            )
            test_connector = Api(test_source)

            # Should not raise exceptions
            test_connector.write_raw(test_data)
            test_connector.write_delta(test_data)

            # Check that files were created
            raw_dir = Path("output") / "raw" / test_source.name
            delta_dir = Path("output") / "delta" / test_source.name

            assert raw_dir.exists(), f"Raw directory should exist for test case {i}"
            assert delta_dir.exists(), f"Delta directory should exist for test case {i}"
