from typing import Any

import pyodbc

from metadata_ingestion import logger
from metadata_ingestion.connectors.base import BaseConnector


class Odbc(BaseConnector):
    """Connector for ODBC data sources."""

    def connect(self) -> None:
        """Establish connection to the ODBC database."""
        try:
            connection_string = self.source.connection.get("odbc_connection_string")
            if connection_string:
                self._connection = pyodbc.connect(connection_string)
                logger.info("Connected to ODBC database using connection string")
            self._is_connected = True
        except (pyodbc.Error, ValueError) as e:
            logger.error(f"Failed to connect to ODBC database: {e}")
            self._is_connected = False

    def disconnect(self) -> None:
        """Close connection to the ODBC database."""
        if self._is_connected:
            self._connection.close()
            self._is_connected = False
            logger.info("Disconnected from ODBC database")

    def fetch_data(self) -> Any:
        """Fetch data from the ODBC database."""
        if not self._is_connected:
            self.connect()

        try:
            cursor = self._connection.cursor()

            # Get query from connection object in JSON configuration
            query = self.source.connection.get("query")

            if query:
                # If query is provided, execute it directly
                cursor.execute(query)
            else:
                # If no query is provided, get the table name and use default query
                table = self.source.connection.get("table", "data")
                cursor.execute(f"SELECT * FROM {table}")

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
