from __future__ import annotations

import importlib
import inspect
from pathlib import Path

from metadata_ingestion import logger
from metadata_ingestion.config.models import Source
from metadata_ingestion.connectors.base import BaseConnector


class ConnectorFactory:
    """Factory for creating connectors based on source type.

    This factory automatically discovers connector classes in the connectors directory
    and provides a unified interface for creating connector instances.
    """

    _connector_cache: dict[str, type[BaseConnector]] = {}
    _instance_cache: dict[str, BaseConnector] = {}

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

        logger.debug(f"Discovering connectors in {connectors_dir}")

        # Skip these files during discovery
        skip_files = {"__init__.py", "factory.py", "base.py"}

        for py_file in connectors_dir.glob("*.py"):
            if py_file.name in skip_files:
                continue

            module_name = f"metadata_ingestion.connectors.{py_file.stem}"
            logger.debug(f"Examining module: {module_name}")

            try:
                module = importlib.import_module(module_name)

                for name, obj in inspect.getmembers(module, inspect.isclass):
                    # Check if this is a valid connector class
                    if (
                        obj != BaseConnector
                        and issubclass(obj, BaseConnector)
                        and obj.__module__ == module_name
                    ):
                        # Store with lowercase key for case-insensitive lookup
                        key = name.lower()
                        connector_classes[key] = obj
                        logger.debug(f"Discovered connector: {name} -> {key}")

            except ImportError as e:
                logger.warning(f"Could not import module {module_name}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error importing {module_name}: {e}")

        if not connector_classes:
            logger.warning("No connector classes discovered")
        else:
            logger.info(
                f"Discovered {len(connector_classes)} connector types: "
                f"{list(connector_classes.keys())}"
            )

        cls._connector_cache = connector_classes
        return connector_classes

    @classmethod
    def create(cls, source: Source) -> BaseConnector:
        """Create and initialize a connector.

        This is the main entry point for creating connectors. It handles caching
        to avoid creating duplicate instances for the same source.

        Args:
            source: Source configuration

        Returns:
            Initialized BaseConnector instance

        Raises:
            ValueError: If the source type is not supported
            Exception: If connector instantiation fails
        """
        if not isinstance(source, Source):
            raise TypeError(f"Expected Source object, got {type(source).__name__}")

        # Check cache first to avoid duplicate instances
        cache_key = f"{source.src_type.lower()}:{source.name}"
        if cache_key in cls._instance_cache:
            logger.debug(f"Returning cached connector for {cache_key}")
            return cls._instance_cache[cache_key]

        # Discover available connectors
        connectors = cls._discover_connectors()

        # Case-insensitive lookup
        src_type_key = source.src_type.lower()

        if src_type_key not in connectors:
            supported_types = sorted(connectors.keys())
            raise ValueError(
                f"Unsupported source type: '{source.src_type}'. "
                f"Supported types are: {', '.join(supported_types)}"
            )

        # Get the class and instantiate it
        connector_class = connectors[src_type_key]
        logger.debug(f"Creating {connector_class.__name__} connector for source '{source.name}'")

        try:
            connector = connector_class(source)
        except Exception as e:
            logger.error(f"Failed to create {connector_class.__name__} connector: {e}")
            raise

        # Cache the instance for future use
        cls._instance_cache[cache_key] = connector
        logger.debug(f"Cached connector instance: {cache_key}")

        return connector

    @classmethod
    def get_supported_types(cls) -> list[str]:
        """Get a list of supported connector types.

        Returns:
            Sorted list of supported connector type names
        """
        connectors = cls._discover_connectors()
        return sorted(connectors.keys())

    @classmethod
    def is_supported_type(cls, src_type: str) -> bool:
        """Check if a source type is supported.

        Args:
            src_type: The source type to check

        Returns:
            True if the source type is supported, False otherwise
        """
        if not isinstance(src_type, str):
            return False
        connectors = cls._discover_connectors()
        return src_type.lower() in connectors

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all caches.

        This is useful for testing or when you want to force rediscovery
        of connectors.
        """
        logger.debug("Clearing connector factory caches")
        cls._connector_cache.clear()
        cls._instance_cache.clear()

    @classmethod
    def get_connector(cls, source_name: str, src_type: str) -> BaseConnector | None:
        """Get a cached connector by name and type.

        Args:
            source_name: The name of the source
            src_type: The type of the source

        Returns:
            The cached connector instance, or None if not found
        """
        cache_key = f"{src_type.lower()}:{source_name}"
        return cls._instance_cache.get(cache_key)

    @classmethod
    def get_cache_stats(cls) -> dict[str, int]:
        """Get statistics about the factory caches.

        Returns:
            Dictionary with cache statistics
        """
        return {
            "connector_types": len(cls._connector_cache),
            "cached_instances": len(cls._instance_cache),
        }
