from __future__ import annotations

import os
import tempfile
from typing import Any

from dagster import (
    DagsterInstance,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    job,
    op,
)

from metadata_ingestion import logger


class DagsterManager:
    """Dagster instance manager for connector orchestration."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._dagster_instance: DagsterInstance | None = None
        self._definitions: list[Definitions] = []
        self._temp_dir: str | None = None

    def get_dagster_instance(self) -> DagsterInstance:
        """Get or create the underlying Dagster instance."""
        if self._dagster_instance is None:
            # Check if we're in test mode
            if self.config.get("test_mode", False):
                # Use ephemeral instance for tests to avoid SQLite file issues
                self._dagster_instance = DagsterInstance.ephemeral()
            else:
                self._temp_dir = self.config.get("temp_dir")
                if not self._temp_dir:
                    self._temp_dir = tempfile.mkdtemp(prefix="dagster_")
                self._dagster_instance = DagsterInstance.local_temp(self._temp_dir)
        return self._dagster_instance

    def add_connector(self, connector) -> None:
        """Create and manage a Dagster job for a given connector."""
        from metadata_ingestion.connectors.base import BaseConnector

        if not isinstance(connector, BaseConnector):
            return

        source_name = connector.source.name

        @op(name=f"{source_name}_connect")
        def connect_op():
            logger.info(f"Connecting to {source_name}")
            connector.connect()

        @op(name=f"{source_name}_fetch")
        def fetch_op():
            logger.info(f"Fetching data from {source_name}")
            return connector.fetch_data()

        @op(name=f"{source_name}_write_raw")
        def write_raw_op(data: Any):
            logger.info(f"Writing raw data for {source_name}")
            connector.write_raw(data)

        @op(name=f"{source_name}_write_delta")
        def write_delta_op(data: Any):
            logger.info(f"Writing delta data for {source_name}")
            connector.write_delta(data)

        @op(name=f"{source_name}_disconnect")
        def disconnect_op():
            logger.info(f"Disconnecting from {source_name}")
            connector.disconnect()

        @job(name=f"{source_name}_job")
        def connector_job():
            """Dagster job for a connector."""
            connect_op()
            data = fetch_op()
            write_raw_op(data)
            write_delta_op(data)
            disconnect_op()

        self._definitions.append(Definitions(jobs=[connector_job]))

        if connector.source.schedule:
            schedule = ScheduleDefinition(
                job=connector_job,
                cron_schedule=connector.source.schedule,
                default_status=DefaultScheduleStatus.RUNNING,
            )
            self._definitions.append(Definitions(schedules=[schedule]))
            logger.info(f"Registered Dagster schedule for {source_name}")
        else:
            logger.info(f"No schedule for {source_name}. Executing job immediately.")
            dagster_instance = self.get_dagster_instance()
            try:
                result = connector_job.execute_in_process(instance=dagster_instance)
                if result.success:
                    logger.info(f"Job '{source_name}' executed successfully in-process.")
                else:
                    logger.error(f"Job '{source_name}' failed in-process execution.")
            except Exception as e:
                logger.error(
                    f"An exception occurred during in-process execution of job '{source_name}': {e}"
                )

    def get_run_status(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get status of recent runs."""
        dagster_instance = self.get_dagster_instance()
        try:
            runs = dagster_instance.get_runs(limit=limit)
            return [
                {"run_id": run.run_id, "job_name": run.job_name, "status": run.status.value}
                for run in runs
            ]
        except Exception as e:
            logger.error(f"Failed to get run status: {e}")
            return []

    def cleanup(self) -> None:
        """Clean up resources."""
        if self._dagster_instance:
            try:
                self._dagster_instance.dispose()
            except Exception as e:
                logger.warning(f"Error disposing Dagster instance: {e}")

        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                import shutil

                shutil.rmtree(self._temp_dir)
            except Exception as e:
                logger.warning(f"Error cleaning up temp directory: {e}")


# Global manager instance
_global_dagster_manager: DagsterManager | None = None


def get_dagster_manager(config: dict[str, Any] | None = None) -> DagsterManager:
    """Get the global DagsterManager instance, optionally reconfiguring it."""
    global _global_dagster_manager

    if config is not None:
        if _global_dagster_manager:
            _global_dagster_manager.cleanup()
        _global_dagster_manager = DagsterManager(config)

    if _global_dagster_manager is None:
        _global_dagster_manager = DagsterManager({"temp_dir": os.getenv("DAGSTER_TEMP_DIR")})

    return _global_dagster_manager
