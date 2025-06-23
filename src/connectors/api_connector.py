from typing import Any

import requests
from metadata_ingestion import logger
from metadata_ingestion.connectors.base import BaseConnector


class Api(BaseConnector):
    """Connector for API data sources."""

    def connect(self) -> None:
        """Establish connection to the API."""
        try:
            response = requests.get(self.source.url)
            response.raise_for_status()
            self._is_connected = True
            logger.info(f"Connected to API at {self.source.url}")
        except requests.RequestException as e:
            logger.error(f"Failed to connect to API: {e}")
            self._is_connected = False

    def disconnect(self) -> None:
        """Close connection to the API."""
        self._is_connected = False
        logger.info("Disconnected from API")

    def fetch_data(self, **kwargs) -> Any:
        """Fetch data from the API."""
        if not self._is_connected:
            self.connect()

        try:
            response = requests.get(self.source.url, params=kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from API: {e}")
            return None

    def write_raw(self, data: Any) -> None:
        """Write data in raw format."""
        logger.info(f"Writing raw data: {data}")

    def write_delta(self, data: Any) -> None:
        """Write data in delta format."""
        logger.info(f"Writing delta data: {data}")
