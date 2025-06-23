import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyodbc
from deltalake import write_deltalake

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
        """Fetch data from the ODBC database.

        Returns:
            A tuple of (data, columns) where data is the fetched rows and
            columns is a list of column names.
        """
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

            # Get column names from cursor description
            columns = [column[0] for column in cursor.description] if cursor.description else []
            data = cursor.fetchall()

            return data, columns
        except pyodbc.Error as e:
            logger.error(f"Failed to fetch data from ODBC database: {e}")
            return None, []

    def write_raw(self, data: Any) -> None:
        """Write data in raw JSON format."""
        # Handle the case where fetch_data returns (data, columns)
        if isinstance(data, tuple) and len(data) == 2:
            rows, columns = data
        else:
            # Fallback for direct data passing
            rows = data
            columns = []

        if not rows:
            logger.warning("No data to write")
            return

        # Create output directory structure
        output_dir = Path("output") / "raw" / self.source.name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = output_dir / f"{self.source.name}_{timestamp}.json"

        # Convert pyodbc.Row objects to dictionaries
        if columns:
            json_data = [dict(zip(columns, row, strict=True)) for row in rows]
        else:
            # If no columns info, just convert to list
            json_data = [list(row) for row in rows]

        # Write data to JSON file
        with open(filename, "w") as f:
            json.dump(json_data, f, indent=2, default=str)

        logger.info(f"Written raw data to {filename}")

    def write_delta(self, data: Any) -> None:
        """Write data in delta lake format."""
        # Handle the case where fetch_data returns (data, columns)
        if isinstance(data, tuple) and len(data) == 2:
            rows, columns = data
        else:
            # Fallback for direct data passing
            rows = data
            columns = []

        if not rows:
            logger.warning("No data to write to Delta Lake")
            return

        # Create output directory structure
        output_dir = Path("output") / "delta" / self.source.name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Convert pyodbc.Row objects to dictionaries
        if columns:
            dict_data = [dict(zip(columns, row, strict=True)) for row in rows]
        else:
            # If no columns, create generic column names
            if rows:
                num_cols = len(rows[0])
                columns = [f"column_{i}" for i in range(num_cols)]
                dict_data = [dict(zip(columns, row, strict=True)) for row in rows]
            else:
                dict_data = []

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
