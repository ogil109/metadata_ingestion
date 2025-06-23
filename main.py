from pathlib import Path

from metadata_ingestion import logger
from metadata_ingestion.config.models import Pipeline
from metadata_ingestion.connectors.factory import ConnectorFactory
from metadata_ingestion.dagster_manager import get_dagster_manager


def load_pipelines(directory: str) -> list[Pipeline]:
    """Load all pipeline JSON files from a directory."""
    pipelines = []
    for file_path in Path(directory).glob("*.json"):
        try:
            pipeline = Pipeline.from_json(file_path)
            pipelines.append(pipeline)
            logger.info(f"Loaded pipeline: {pipeline.name}")
        except Exception as e:
            logger.error(f"Failed to load pipeline from {file_path}: {e}")
    return pipelines


def create_connectors(pipelines: list[Pipeline]) -> list:
    """Create connectors from the loaded pipelines."""
    connectors = []
    for pipeline in pipelines:
        try:
            logger.info(f"Creating connectors for pipeline: {pipeline.name}")
            for source in pipeline.sources:
                # The factory creates the connector, which then auto-registers with the DagsterManager.
                connector = ConnectorFactory.create(source)
                connectors.append(connector)
                logger.info(f"Created connector for source: {source.name}")
        except Exception as e:
            logger.error(f"Failed to create connectors for pipeline {pipeline.name}: {e}")
    return connectors


def main() -> None:
    """Main function to load pipelines, create connectors, and trigger Dagster execution."""
    dagster_manager = get_dagster_manager()

    # Load pipelines from the pipelines directory
    pipelines_directory = "pipelines"
    pipelines = load_pipelines(pipelines_directory)

    if not pipelines:
        logger.error("No pipelines found. Exiting.")
        return

    # Create connectors. This will now automatically register and execute Dagster jobs.
    connectors = create_connectors(pipelines)

    if not connectors:
        logger.error("No connectors were created. Exiting.")
        return

    logger.info(f"Successfully created and registered {len(connectors)} connectors.")

    # Get run status from Dagster
    run_status = dagster_manager.get_run_status()

    if run_status:
        logger.info("Recent Dagster runs:")
        for run in run_status:
            logger.info(
                f"  Job: {run['job_name']}, Status: {run['status']}, Run ID: {run['run_id']}"
            )
    else:
        logger.info("No recent Dagster runs found")

    # Clean up resources
    dagster_manager.cleanup()


if __name__ == "__main__":
    main()
