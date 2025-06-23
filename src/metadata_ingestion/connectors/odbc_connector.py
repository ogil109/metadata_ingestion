from typing import Any

import pyodbc

from metadata_ingestion import logger
from metadata_ingestion.connectors.base import BaseConnector


class Odbc(BaseConnector):
    """Connector for ODBC data sources."""

    def connect(self) -> None:
        """Establish connection to the ODBC database."""
        try:
            # Check if we have a direct connection string
            connection_string = self.source.connection.get("odbc_connection_string")

            if connection_string:
                # Use the provided connection string directly
                self._connection = pyodbc.connect(connection_string)
                logger.info("Connected to ODBC database using connection string")
            else:
                # Build connection string from individual components
                driver = self.source.connection.get("driver")
                server = self.source.connection.get("server")
                database = self.source.connection.get("database")
                username = self.source.connection.get("username")
                password = self.source.connection.get("password")

                if not all([driver, server, database]):
                    raise ValueError(
                        "Missing required ODBC connection parameters: driver, server, database"
                    )

                connection_parts = [f"DRIVER={driver}", f"SERVER={server}", f"DATABASE={database}"]

                if username:
                    connection_parts.append(f"UID={username}")
                if password:
                    connection_parts.append(f"PWD={password}")

                connection_string = ";".join(connection_parts)
                self._connection = pyodbc.connect(connection_string)
                logger.info(f"Connected to ODBC database {database}")

            self._is_connected = True
        except pyodbc.Error as e:
            logger.error(f"Failed to connect to ODBC database: {e}")
            self._is_connected = False
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
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
