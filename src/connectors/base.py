from abc import ABC, abstractmethod
from typing import Any

from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, job, op
from metadata_ingestion import logger
from metadata_ingestion.config.models import Source
from metadata_ingestion.dagster_manager import get_dagster_manager


class BaseConnector(ABC):
    """Abstract base class for all connectors with integrated orchestration."""

    def __init__(self, source: Source) -> None:
        """Initialize connector with source configuration."""
        self.source = source
        self._dagster_job = None
        self._dagster_schedule = None
        self._is_connected = False
        self._create_dagster_definitions()

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

    def _create_dagster_definitions(self) -> None:
        """Create Dagster job and schedule definitions for this connector."""
        # Create the ops for this connector
        connect_op = self._create_connect_op()
        fetch_op = self._create_fetch_op()
        write_raw_op = self._create_write_raw_op()
        write_delta_op = self._create_write_delta_op()
        disconnect_op = self._create_disconnect_op()

        # Create the job
        @job(name=f"{self.source.name}_job")
        def connector_job():
            """Dagster job for this connector."""
            connect_result = connect_op()
            fetch_result = fetch_op(connect_result)
            write_raw_op(fetch_result)
            write_delta_op(fetch_result)
            disconnect_op(connect_result)

        self._dagster_job = connector_job

        # Create schedule if specified
        if self.source.schedule:
            self._dagster_schedule = ScheduleDefinition(
                job=self._dagster_job,
                cron_schedule=self.source.schedule,
                default_status=DefaultScheduleStatus.RUNNING,
            )

        # Create definitions and register with Dagster manager
        schedules = [self._dagster_schedule] if self._dagster_schedule else []
        definitions = Definitions(
            jobs=[self._dagster_job],
            schedules=schedules,
        )

        # Register with Dagster manager
        get_dagster_manager().register_definitions(definitions)
        logger.info(f"Registered Dagster definitions for {self.source.name}")

    def _create_connect_op(self):
        """Create Dagster op for connecting to the source."""

        @op(name=f"{self.source.name}_connect")
        def connect_op():
            """Connect to the data source."""
            logger.info(f"Connecting to {self.source.name}")
            self.connect()
            return {"connected": True, "source_name": self.source.name}

        return connect_op

    def _create_fetch_op(self):
        """Create Dagster op for fetching data."""

        @op(name=f"{self.source.name}_fetch")
        def fetch_op(connect_result):
            """Fetch data from the source."""
            logger.info(f"Fetching data from {self.source.name}")
            data = self.fetch_data()
            return {"data": data, "source_name": self.source.name}

        return fetch_op

    def _create_write_raw_op(self):
        """Create Dagster op for writing raw data."""

        @op(name=f"{self.source.name}_write_raw")
        def write_raw_op(fetch_result):
            """Write raw data."""
            logger.info(f"Writing raw data for {self.source.name}")
            self.write_raw(fetch_result["data"])
            return {"written_raw": True, "source_name": self.source.name}

        return write_raw_op

    def _create_write_delta_op(self):
        """Create Dagster op for writing delta data."""

        @op(name=f"{self.source.name}_write_delta")
        def write_delta_op(fetch_result):
            """Write delta data."""
            logger.info(f"Writing delta data for {self.source.name}")
            self.write_delta(fetch_result["data"])
            return {"written_delta": True, "source_name": self.source.name}

        return write_delta_op

    def _create_disconnect_op(self):
        """Create Dagster op for disconnecting from the source."""

        @op(name=f"{self.source.name}_disconnect")
        def disconnect_op(connect_result):
            """Disconnect from the data source."""
            logger.info(f"Disconnecting from {self.source.name}")
            self.disconnect()
            return {"disconnected": True, "source_name": self.source.name}

        return disconnect_op

    def get_dagster_job(self):
        """Get the Dagster job for this connector."""
        return self._dagster_job

    def get_dagster_schedule(self):
        """Get the Dagster schedule for this connector."""
        return self._dagster_schedule
