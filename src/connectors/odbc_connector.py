import pyodbc

from src.config.models import Source
from src.connectors.factory import Connector


class OdbcConnector(Connector):
    def __init__(self, source: Source) -> None:
        super().__init__(source)
        self.connection_string = source.connection.get("odbc_connection_string", "")
        self.connection = None

    def __enter__(self):
        """Context manager entry - connects to the database."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - disconnects from the database."""
        self.disconnect()

    def connect(self) -> None:
        """Establish an ODBC connection using the connection string."""
        self.connection = pyodbc.connect(self.connection_string)

    def disconnect(self) -> None:
        """Close the ODBC connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def fetch_data(self, sql: str) -> pyodbc.Cursor:
        """Execute custom SQL and return cursor for result retrieval."""
        if not self.connection:
            raise Exception("Connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(sql)
        return cursor
