"""
This script loads pipelines from a directory and creates connectors for each source.
It then automatically registers and executes Dagster jobs for sources without schedules.
It's an example of a metadata ingestion process based on a directory of JSON files.

Deployment logic should replicate this script.
"""

from pathlib import Path

from metadata_ingestion import logger
from metadata_ingestion.config.models import Pipeline
from metadata_ingestion.connectors.base import BaseConnector
from metadata_ingestion.connectors.factory import ConnectorFactory
from metadata_ingestion.dagster_manager import get_dagster_manager
from metadata_ingestion.db_utils import setup_database


def load_pipelines(directory: str) -> list[Pipeline]:
    """Load all pipeline JSON files from a directory.

    Args:
        directory: Path to the directory containing pipeline JSON files

    Returns:
        List of loaded Pipeline objects
    """
    pipelines_path = Path(directory)

    if not pipelines_path.exists():
        logger.error(f"Pipelines directory '{directory}' does not exist")
        return []

    pipelines = []
    json_files = list(pipelines_path.glob("*.json"))

    if not json_files:
        logger.warning(f"No JSON files found in '{directory}'")
        return []

    for file_path in json_files:
        try:
            pipeline = Pipeline.from_json(file_path)
            pipelines.append(pipeline)
            logger.info(f"Loaded pipeline: {pipeline.name}")
        except Exception as e:
            logger.error(f"Failed to load pipeline from {file_path}: {e}")

    return pipelines


def create_connectors(pipelines: list[Pipeline]) -> list[BaseConnector]:
    """Create connectors from the loaded pipelines.

    Args:
        pipelines: List of Pipeline objects

    Returns:
        List of created connector instances
    """
    connectors = []

    for pipeline in pipelines:
        logger.info(f"Creating connectors for pipeline: {pipeline.name}")

        for source in pipeline.sources:
            try:
                # The factory creates the connector, which then
                # auto-registers with the DagsterManager
                connector = ConnectorFactory.create(source)
                connectors.append(connector)
                logger.info(f"Created {source.src_type} connector for source: {source.name}")
            except Exception as e:
                logger.error(
                    f"Failed to create connector for source '{source.name}' "
                    f"in pipeline '{pipeline.name}': {e}"
                )

    return connectors


def display_run_summary(dagster_manager) -> None:
    """Display a summary of recent Dagster runs.

    Args:
        dagster_manager: The DagsterManager instance
    """
    try:
        run_status = dagster_manager.get_run_status(limit=10)

        if run_status:
            logger.info("Recent Dagster runs:")
            for run in run_status:
                logger.info(
                    f"  Job: {run['job_name']:<30} "
                    f"Status: {run['status']:<10} "
                    f"Run ID: {run['run_id']}"
                )
        else:
            logger.info("No recent Dagster runs found")
    except Exception as e:
        logger.error(f"Failed to retrieve run status: {e}")


def main() -> None:
    """Main function to orchestrate the metadata ingestion process.

    This function:
    1. Sets up the database
    2. Loads pipeline configurations from JSON files
    3. Creates connectors based on the pipeline sources
    4. Automatically executes Dagster jobs for sources without schedules
    5. Displays a summary of recent runs
    6. Cleans up resources
    """
    logger.info("Starting metadata ingestion process...")

    # Set up the database before any other operations
    setup_database()

    # Initialize Dagster manager
    dagster_manager = get_dagster_manager()

    try:
        # Load pipelines from the pipelines directory
        pipelines_directory = "pipelines"
        pipelines = load_pipelines(pipelines_directory)

        if not pipelines:
            logger.error("No valid pipelines found. Exiting.")
            return

        logger.info(f"Loaded {len(pipelines)} pipeline(s)")

        # Create connectors (automatically registers and executes Dagster jobs)
        connectors = create_connectors(pipelines)

        if not connectors:
            logger.error("No connectors were created. Exiting.")
            return

        logger.info(f"Successfully created and registered {len(connectors)} connector(s)")

        # Display run summary
        display_run_summary(dagster_manager)

    except Exception as e:
        logger.error(f"Unexpected error during execution: {e}")
        raise
    finally:
        # Always clean up resources
        logger.info("Cleaning up resources...")
        dagster_manager.cleanup()
        logger.info("Metadata ingestion process completed")


if __name__ == "__main__":
    main()
