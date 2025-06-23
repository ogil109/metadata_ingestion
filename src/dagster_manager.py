import os
import tempfile
from typing import Any

from dagster import (
    DagsterInstance,
    Definitions,
)
from metadata_ingestion import logger


class DagsterManager:
    """Dagster instance manager for connector orchestration."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._instance: DagsterInstance | None = None
        self._definitions: list[Definitions] = []
        self._temp_dir: str | None = None

    def get_instance(self) -> DagsterInstance:
        """Get or create local Dagster instance."""
        if self._instance is None:
            self._temp_dir = self.config.get("temp_dir")
            if not self._temp_dir:
                self._temp_dir = tempfile.mkdtemp(prefix="dagster_")
            self._instance = DagsterInstance.local_temp(self._temp_dir)

        return self._instance

    def register_definitions(self, definitions: Definitions) -> None:
        """Register Definitions with the Dagster instance."""
        self._definitions.append(definitions)
        logger.info(
            f"Registered {len(definitions.jobs)} jobs and {len(definitions.schedules)} schedules"
        )
        self._submit_local_definitions(definitions)

    def _submit_local_definitions(self, definitions: Definitions) -> None:
        """Submit definitions to local instance."""
        instance = self.get_instance()

        for job_def in definitions.jobs:
            try:
                # Check if job has a schedule
                job_schedule = (
                    next((s for s in definitions.schedules if s.job == job_def), None)
                    if definitions.schedules
                    else None
                )

                if job_schedule:
                    logger.info(
                        f"Schedule registered: {job_def.name} ({job_schedule.cron_schedule})"
                    )
                else:
                    # Execute immediately for non-scheduled jobs
                    run = instance.create_run_for_job(job_def)
                    instance.submit_run(run.run_id, job_def)
                    logger.info(f"Job executed: {job_def.name}")

            except Exception as e:
                logger.error(f"Failed to submit job {job_def.name}: {e}")

    def get_run_status(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get status of recent runs."""
        instance = self.get_instance()

        try:
            runs = instance.get_runs(limit=limit)
            return [
                {
                    "run_id": run.run_id,
                    "job_name": run.job_name,
                    "status": run.status.value,
                    "start_time": run.start_time,
                    "end_time": run.end_time,
                }
                for run in runs
            ]
        except Exception as e:
            logger.error(f"Failed to get run status: {e}")
            return []

    def cleanup(self) -> None:
        """Clean up resources."""
        if self._instance:
            try:
                self._instance.dispose()
            except Exception as e:
                logger.warning(f"Error disposing instance: {e}")

        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                import shutil

                shutil.rmtree(self._temp_dir)
            except Exception as e:
                logger.warning(f"Error cleaning up temp directory: {e}")


# Global manager instance
_dagster_manager: DagsterManager | None = None


def get_dagster_manager(config: dict[str, Any] | None = None) -> DagsterManager:
    """Get the global Dagster manager instance, optionally reconfiguring it."""
    global _dagster_manager

    if config is not None:
        if _dagster_manager:
            _dagster_manager.cleanup()
        _dagster_manager = DagsterManager(config)

    if _dagster_manager is None:
        _dagster_manager = DagsterManager(
            {
                "temp_dir": os.getenv("DAGSTER_TEMP_DIR"),
            }
        )

    return _dagster_manager
