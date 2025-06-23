from typing import Any

import requests

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

            response = requests.get(endpoint)
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
        """Write data in raw format."""
        logger.info(f"Writing raw data: {data}")

    def write_delta(self, data: Any) -> None:
        """Write data in delta format."""
        logger.info(f"Writing delta data: {data}")
