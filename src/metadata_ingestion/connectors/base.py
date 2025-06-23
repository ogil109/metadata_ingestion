from abc import ABC, abstractmethod
from typing import Any

from metadata_ingestion.config.models import Source


class BaseConnector(ABC):
    """Abstract base class for all connectors."""

    def __init__(self, source: Source) -> None:
        """Initialize connector with source configuration and auto-register with Dagster."""
        from metadata_ingestion.dagster_manager import get_dagster_manager

        self.source = source
        self._is_connected = False
        # Auto-register the connector instance with the Dagster manager.
        get_dagster_manager().add_connector(self)

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source."""
        pass

    @abstractmethod
    def fetch_data(self, **kwargs) -> Any:
        """Fetch data from the source."""
        pass

    @abstractmethod
    def write_raw(self, data: Any) -> None:
        """Write data in raw format."""
        pass

    @abstractmethod
    def write_delta(self, data: Any) -> None:
        """Write data in delta format."""
        pass
