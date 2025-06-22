import importlib
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, job, op

from config.models import Source


class Connector:
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


class ConnectorFactory:
    """Gateway for instantiating the correct connector based on source type."""

    _connector_cache: dict[str, type[Connector]] = {}

    @classmethod
    def _discover_connectors(cls) -> dict[str, type[Connector]]:
        """Dynamically discover all available connector classes.

        Returns:
            dict: Mapping of src_type to connector class (for validation purposes)
        """
        if cls._connector_cache:
            return cls._connector_cache

        # Scan all Python files in connectors dir and find Connector subclasses
        connectors_dir = Path(__file__).parent
        connector_classes = {}

        for py_file in connectors_dir.glob("*.py"):
            if py_file.name in ("__init__.py", "factory.py"):
                continue

            module_name = f"src.connectors.{py_file.stem}"

            module = importlib.import_module(module_name)

            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    obj != Connector
                    and issubclass(obj, Connector)
                    and obj.__module__ == module_name
                ):
                    # Use class name as the src_type
                    src_type = name.lower()
                    connector_classes[src_type] = obj

        cls._connector_cache = connector_classes
        return connector_classes

    @classmethod
    def create(cls, source: Source) -> Connector:
        """Create and return the appropriate connector instance based on source type if supported.

        Args:
            source: Source instance containing connection details and type information

        Returns:
            Connector: Instance of the appropriate connector

        Raises:
            ValueError: If the src_type is not supported
        """
        connectors = cls._discover_connectors()

        if source.src_type not in connectors:
            supported_types = list(connectors.keys())
            raise ValueError(
                f"Unsupported source type: '{source.src_type}'. "
                f"Supported types are: {', '.join(sorted(supported_types))}"
            )

        connector_class = connectors[source.src_type]
        return connector_class(source)

    @classmethod
    def get_supported_types(cls) -> list[str]:
        """Get a list of supported connector types.

        Returns:
            list[str]: List of supported source types
        """
        connectors = cls._discover_connectors()
        return sorted(connectors.keys())

    @classmethod
    def is_supported_type(cls, src_type: str) -> bool:
        """Check if a source type is supported.

        Args:
            src_type: The source type to check

        Returns:
            bool: True if the source type is supported, False otherwise
        """
        connectors = cls._discover_connectors()
        return src_type in connectors

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the connector cache. Useful for testing or dynamic reloading."""
        cls._connector_cache.clear()
