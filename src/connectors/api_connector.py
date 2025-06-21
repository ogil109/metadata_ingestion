import requests

from src.config.models import Source
from src.connectors.factory import BaseConnector


class ApiConnector(BaseConnector):
    def __init__(self, source: Source) -> None:
        super().__init__(source)
        self.api_url = source.connection.get("api_url", "")
        self.api_key = source.connection.get("api_key", "")
        self.session = None
        self.default_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def __enter__(self):
        """Context manager entry - connects to the API."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - disconnects from the API."""
        self.disconnect()

    def connect(self) -> None:
        """Establish an API connection by creating a session."""
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)

    def disconnect(self) -> None:
        """Close the API connection."""
        if self.session:
            self.session.close()
            self.session = None

    def fetch_data(
        self,
        endpoint: str = "",
        params: dict = None,
        method: str = "GET",
        data: dict = None,
        headers: dict = None,
        pagination: bool = False,
    ) -> dict:
        """Fetch data from the API source.

        Args:
            endpoint: API endpoint to call
            params: Query parameters
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            data: Data to send in the request body
            headers: Additional headers
            pagination: Whether to handle pagination

        Returns:
            JSON response from the API
        """
        if not self.session:
            raise Exception("Connection not established. Call connect() first.")

        url = f"{self.api_url}/{endpoint}" if endpoint else self.api_url
        request_headers = {**self.default_headers, **(headers or {})}

        # Handle different HTTP methods
        if method.upper() == "GET":
            response = self.session.get(url, params=params, headers=request_headers)
        elif method.upper() == "POST":
            response = self.session.post(url, params=params, json=data, headers=request_headers)
        elif method.upper() == "PUT":
            response = self.session.put(url, params=params, json=data, headers=request_headers)
        elif method.upper() == "DELETE":
            response = self.session.delete(url, params=params, headers=request_headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()

        # Handle pagination (only valid if JSON:API standard is used)
        if pagination:
            result = response.json()
            # Assuming the API uses a common pagination structure
            while "next" in result.get("links", {}):
                next_url = result["links"]["next"]
                response = self.session.get(next_url, headers=request_headers)
                response.raise_for_status()
                result = response.json()
                # Append the new data to the existing result
            return result
        else:
            return response.json()
