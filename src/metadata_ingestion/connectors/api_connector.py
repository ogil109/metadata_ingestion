import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import requests
from deltalake import write_deltalake

from metadata_ingestion import logger
from metadata_ingestion.connectors.base import BaseConnector


class Api(BaseConnector):
    """Connector for API data sources."""

    def connect(self) -> None:
        """Establish connection to the API."""
        try:
            endpoint = self.source.connection.get("endpoint")
            if not endpoint:
                raise ValueError("API endpoint not specified in connection configuration")

            # Use the same request type as configured for the actual data fetch
            request_type = self.source.connection.get("type", "GET").upper()

            if request_type == "GET":
                params = self.source.connection.get("args", {})
                response = requests.get(endpoint, params=params)
            elif request_type == "POST":
                body = self.source.connection.get("body", {})
                response = requests.post(endpoint, json=body)
            else:
                raise ValueError(f"Unsupported request type: {request_type}")

            response.raise_for_status()
            self._is_connected = True
            logger.info(f"Connected to API at {endpoint}")
        except requests.RequestException as e:
            logger.error(f"Failed to connect to API: {e}")
            self._is_connected = False
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            self._is_connected = False

    def disconnect(self) -> None:
        """Close connection to the API."""
        self._is_connected = False
        logger.info("Disconnected from API")

    def fetch_data(self) -> Any:
        """Fetch data from the API.

        Returns:
            The JSON response from the API, or None if the request failed.
        """
        if not self._is_connected:
            self.connect()

        try:
            endpoint = self.source.connection.get("endpoint")
            if not endpoint:
                raise ValueError("API endpoint not specified in connection configuration")

            # Use the same request type as configured for the actual data fetch
            request_type = self.source.connection.get("type", "GET").upper()

            if request_type == "GET":
                # For GET requests, use 'args' from the config as query parameters.
                params = self.source.connection.get("args", {})
                response = requests.get(endpoint, params=params)
            elif request_type == "POST":
                # For POST requests, use 'body' from the config as the JSON payload.
                body = self.source.connection.get("body", {})
                response = requests.post(endpoint, json=body)
            else:
                raise ValueError(f"Unsupported request type: {request_type}")

            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from API: {e}")
            return None
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            return None

    def write_raw(self, data: Any) -> None:
        """Write data in raw JSON format."""
        if not data:
            logger.warning("No data to write")
            return

        # Create output directory structure
        output_dir = Path("output") / "raw" / self.source.name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = output_dir / f"{self.source.name}_{timestamp}.json"

        # Write data to JSON file
        with open(filename, "w") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(f"Written raw data to {filename}")

    def write_delta(self, data: Any) -> None:
        """Write data in delta lake format."""
        if not data:
            logger.warning("No data to write to Delta Lake")
            return

        # Create output directory structure
        output_dir = Path("output") / "delta" / self.source.name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Normalize data to list of dictionaries for Delta Lake
        if isinstance(data, dict):
            # If it's a single dict, wrap it in a list
            dict_data = [data]
        elif isinstance(data, list):
            # If it's already a list, check if items are dicts
            if data and isinstance(data[0], dict):
                dict_data = data
            else:
                # Convert list items to dicts with generic keys
                dict_data = [{"value": item} for item in data]
        else:
            # For other types, wrap in a dict
            dict_data = [{"value": data}]

        # Check if we have meaningful data to write
        if not dict_data:
            logger.warning("No data to write to Delta Lake - skipping")
            return

        # Filter out empty nested objects that cause Delta Lake issues
        def clean_record(record):
            if not isinstance(record, dict):
                return record
            cleaned = {}
            for key, value in record.items():
                if isinstance(value, dict) and not value:
                    # Skip empty dictionaries
                    continue
                elif isinstance(value, list) and not value:
                    # Skip empty lists
                    continue
                else:
                    cleaned[key] = value
            return cleaned if cleaned else {"placeholder": "empty_record"}

        dict_data = [clean_record(record) for record in dict_data]

        # Convert to PyArrow table
        try:
            # Create a PyArrow table from the data
            table = pa.Table.from_pylist(dict_data)

            # Check if Delta table exists
            delta_log_dir = output_dir / "_delta_log"
            if delta_log_dir.exists():
                # If table exists, use merge mode to handle schema changes
                try:
                    write_deltalake(str(output_dir), table, mode="append")
                except Exception as schema_error:
                    logger.warning(f"Schema mismatch, overwriting table: {schema_error}")
                    # If schema doesn't match, overwrite the table
                    write_deltalake(str(output_dir), table, mode="overwrite")
            else:
                # If table doesn't exist, create it
                write_deltalake(str(output_dir), table, mode="overwrite")

            logger.info(f"Written data to Delta Lake at {output_dir}")
        except Exception as e:
            logger.error(f"Failed to write to Delta Lake: {e}")
            raise
