import logging
import os
from typing import Any

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def initialize_dagster(config: dict[str, Any] | None = None) -> None:
    """Initialize the Dagster manager for the package.

    Args:
        config: Configuration dictionary for the Dagster instance
               If None, uses a default "DAGSTER_TEMP_DIR" env var for configuration
    """
    from metadata_ingestion.dagster_manager import get_dagster_manager

    get_dagster_manager(config)
    logger.info("Dagster manager initialized for local execution")


def cleanup_dagster() -> None:
    """Clean up Dagster resources.

    This should be called when shutting down the application to ensure
    proper cleanup of temporary directories and resources.
    """
    from metadata_ingestion.dagster_manager import get_dagster_manager

    manager = get_dagster_manager()
    manager.cleanup()
    logger.info("Dagster resources cleaned up")


# Initialize Dagster manager on package import
initialize_dagster()
