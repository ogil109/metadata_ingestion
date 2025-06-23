import importlib
import inspect
from pathlib import Path

from metadata_ingestion import logger
from metadata_ingestion.config.models import Source
from metadata_ingestion.connectors.base import BaseConnector


class ConnectorFactory:
    """Factory for creating connectors based on source type."""

    _connector_cache = {}
    _instance_cache = {}

    @classmethod
    def _discover_connectors(cls) -> dict[str, type[BaseConnector]]:
        """Dynamically discover all available connector classes.

        Returns:
            Dictionary mapping lowercase connector names to class objects.
            Example: {'odbc': <class Odbc>, 'api': <class Api>}
        """
        if cls._connector_cache:
            return cls._connector_cache

        connectors_dir = Path(__file__).parent
        connector_classes = {}

        for py_file in connectors_dir.glob("*.py"):
            if py_file.name in ("__init__.py", "factory.py", "base.py"):
                continue

            module_name = f"metadata_ingestion.connectors.{py_file.stem}"

            try:
                module = importlib.import_module(module_name)

                for name, obj in inspect.getmembers(module, inspect.isclass):
                    # Avoid circular imports
                    from metadata_ingestion.connectors.base import BaseConnector

                    if (
                        obj != BaseConnector
                        and issubclass(obj, BaseConnector)
                        and obj.__module__ == module_name
                    ):
                        # Store with lowercase key for direct lookup
                        connector_classes[name.lower()] = obj
                        logger.debug(f"Discovered connector: {name} -> {name.lower()}")

            except Exception as e:
                logger.warning(f"Failed to import module {module_name}: {e}")

        cls._connector_cache = connector_classes
        return connector_classes

    @classmethod
    def create(cls, source: Source) -> BaseConnector:
        """Create and initialize a connector.

        This is the main entry point for creating connectors.

        Args:
            source: Source configuration

        Returns:
            Initialized BaseConnector
        """
        # Check cache first
        cache_key = f"{source.src_type}:{source.name}"
        if cache_key in cls._instance_cache:
            return cls._instance_cache[cache_key]

        # Discover available connectors
        connectors = cls._discover_connectors()

        # Direct lookup using lowercase key
        src_type_key = source.src_type.lower()

        if src_type_key not in connectors:
            supported_types = list(connectors.keys())
            raise ValueError(
                f"Unsupported source type: '{source.src_type}'. "
                f"Supported types are: {', '.join(sorted(supported_types))}"
            )

        # Get the class object and instantiate it
        connector_class = connectors[src_type_key]
        connector = connector_class(source)

        # Cache the instance
        cls._instance_cache[cache_key] = connector

        return connector

    @classmethod
    def get_supported_types(cls) -> list[str]:
        """Get a list of supported connector types."""
        connectors = cls._discover_connectors()
        return sorted(connectors.keys())

    @classmethod
    def is_supported_type(cls, src_type: str) -> bool:
        """Check if a source type is supported."""
        connectors = cls._discover_connectors()
        return src_type.lower() in connectors

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all caches."""
        cls._connector_cache.clear()
        cls._instance_cache.clear()

    @classmethod
    def get_connector(cls, source_name: str, src_type: str) -> BaseConnector | None:
        """Get a cached connector by name and type."""
        cache_key = f"{src_type}:{source_name}"
        return cls._instance_cache.get(cache_key)
