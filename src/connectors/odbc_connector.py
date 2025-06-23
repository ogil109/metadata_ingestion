from typing import Any

import pyodbc
from metadata_ingestion import logger
from metadata_ingestion.connectors.base import BaseConnector


class Odbc(BaseConnector):
    """Connector for ODBC data sources."""

    def connect(self) -> None:
        """Establish connection to the ODBC database."""
        try:
            self._connection = pyodbc.connect(
                f"DRIVER={self.source.driver};"
                f"SERVER={self.source.server};"
                f"DATABASE={self.source.database};"
                f"UID={self.source.username};"
                f"PWD={self.source.password}"
            )
            self._is_connected = True
            logger.info(f"Connected to ODBC database {self.source.database}")
        except pyodbc.Error as e:
            logger.error(f"Failed to connect to ODBC database: {e}")
            self._is_connected = False

    def disconnect(self) -> None:
        """Close connection to the ODBC database."""
        if self._is_connected:
            self._connection.close()
            self._is_connected = False
            logger.info("Disconnected from ODBC database")

    def fetch_data(self, **kwargs) -> Any:
        """Fetch data from the ODBC database."""
        if not self._is_connected:
            self.connect()

        try:
            cursor = self._connection.cursor()
            cursor.execute(kwargs.get("query", "SELECT * FROM data"))
            return cursor.fetchall()
        except pyodbc.Error as e:
            logger.error(f"Failed to fetch data from ODBC database: {e}")
            return None

    def write_raw(self, data: Any) -> None:
        """Write data in raw format."""
        logger.info(f"Writing raw data: {data}")

    def write_delta(self, data: Any) -> None:
        """Write data in delta format."""
        logger.info(f"Writing delta data: {data}")
