from collections.abc import Callable
from typing import Any

from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, job, op

from config.models import Source


class BaseConnector:
    def __init__(self, source: Source) -> None:
        self.source = source
        self._job_func: Callable[[], Any] | None = None
        self._dagster_job = None
        self._dagster_schedule = None

    def _create_dagster_schedule(self) -> ScheduleDefinition | None:
        """Create a Dagster schedule if a cron schedule is configured.

        Returns:
            ScheduleDefinition if schedule is configured, None otherwise
        """
        if not self.source.schedule or not self._dagster_job:
            return None

        self._dagster_schedule = ScheduleDefinition(
            job=self._dagster_job,
            cron_schedule=self.source.schedule,
            default_status=DefaultScheduleStatus.RUNNING,
        )

        return self._dagster_schedule

    def create_dagster_job(self, job_func: Callable[[], Any]) -> None:
        """Create a Dagster job from the provided function.

        Args:
            job_func: The function to wrap as a Dagster job
        """
        self._job_func = job_func

        @op(name=f"ingest_{self.source.name}_op")
        def ingest_op():
            return job_func()

        @job(name=f"ingest_{self.source.name}_job")
        def ingest_job():
            ingest_op()

        self._dagster_job = ingest_job
        self._dagster_schedule = self._create_dagster_schedule()

    def start_scheduling(self, job_func: Callable[[], Any]) -> None:
        """Start execution of the job function.

        If the source has a schedule, creates a Dagster job and schedule.
        If no schedule is provided, the job will run immediately once.

        Args:
            job_func: The function to run
        """
        # If there's no schedule, run the job once immediately
        if not self.source.schedule:
            job_func()
            return

        self.create_dagster_job(job_func)

    def get_dagster_definitions(self) -> Definitions | None:
        """Get Dagster definitions for this connector.

        Returns:
            Definitions object containing jobs and schedules, or None if no schedule
        """
        if not self._dagster_job:
            return None

        jobs = [self._dagster_job]
        schedules = [self._dagster_schedule] if self._dagster_schedule else []

        return Definitions(
            jobs=jobs,
            schedules=schedules,
        )

    def stop_scheduling(self) -> None:
        """Stop any scheduled jobs.

        Note: In Dagster, schedules are managed by the daemon.
        This method clears the internal references.
        """
        self._dagster_job = None
        self._dagster_schedule = None
        self._job_func = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - ensures cleanup."""
        self.stop_scheduling()
