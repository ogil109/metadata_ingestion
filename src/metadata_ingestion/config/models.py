from __future__ import annotations

import json
import re
from pathlib import Path

# Cron schedule pattern for validation
CRON_PATTERN = re.compile(r"^[\d\*\-,/]+ [\d\*\-,/]+ [\d\*\-,/]+ [\d\*\-,/]+ [\d\*\-,/]+$")


class Pipeline:
    """Represents a data pipeline configuration containing multiple sources."""

    def __init__(self, name: str, sources: list[Source]) -> None:
        """Initialize a Pipeline.

        Args:
            name: The pipeline name
            sources: List of data sources in this pipeline
        """
        if not name:
            raise ValueError("Pipeline name cannot be empty")
        if not sources:
            raise ValueError("Pipeline must have at least one source")

        self.name = name
        self.sources = sources

    @classmethod
    def from_json(cls, path: str | Path) -> Pipeline:
        """Load a Pipeline from a JSON file.

        Args:
            path: Path to the JSON file

        Returns:
            Pipeline instance

        Raises:
            ValueError: If the JSON is invalid or missing required fields
            FileNotFoundError: If the file doesn't exist
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Pipeline file not found: {path}")

        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {path}: {str(e)}") from e
        except Exception as e:
            raise ValueError(f"Error reading {path}: {str(e)}") from e

        # Convert to list if single source to allow iteration
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise ValueError(f"Pipeline data must be a dict or list, got {type(data).__name__}")

        # Build sources with validation
        sources = []
        for idx, source_data in enumerate(data):
            if not isinstance(source_data, dict):
                raise ValueError(f"Source {idx} must be a dict, got {type(source_data).__name__}")
            try:
                sources.append(Source(**source_data))
            except Exception as e:
                raise ValueError(f"Error in source {idx}: {str(e)}") from e

        # Use filename without extension as pipeline name
        pipeline_name = path.stem
        return cls(pipeline_name, sources)

    def __repr__(self) -> str:
        return f"Pipeline(name='{self.name}', sources={len(self.sources)})"


class Source:
    """Represents a data source configuration."""

    def __init__(
        self, name: str, src_type: str, connection: dict, schedule: str | None = None
    ) -> None:
        """Initialize a Source.

        Args:
            name: The source name
            src_type: The connector type (e.g., 'odbc', 'api')
            connection: Connection configuration dictionary
            schedule: Optional cron schedule (None means run immediately)

        Raises:
            ValueError: If any validation fails
        """
        # Validate required fields
        if not name:
            raise ValueError("Source name cannot be empty")
        if not src_type:
            raise ValueError("Source type cannot be empty")
        if not isinstance(connection, dict):
            raise ValueError(f"Connection must be a dict, got {type(connection).__name__}")
        if not connection:
            raise ValueError("Connection configuration cannot be empty")

        # Validate src_type
        self._validate_src_type(src_type)

        # Validate schedule format if provided
        if schedule is not None:
            if not isinstance(schedule, str):
                raise ValueError(f"Schedule must be a string, got {type(schedule).__name__}")
            if not CRON_PATTERN.match(schedule):
                raise ValueError(
                    f"Invalid cron schedule: '{schedule}'. "
                    "Expected format: '* * * * *' (minute hour day month weekday)"
                )

        self.name = name
        self.src_type = src_type
        self.connection = connection
        self.schedule = schedule

    def _validate_src_type(self, src_type: str) -> None:
        """Validate that the src_type has a corresponding connector class.

        Args:
            src_type: The source type to validate

        Raises:
            ValueError: If the src_type is not supported
        """
        # Import here to avoid circular imports
        from metadata_ingestion.connectors.factory import ConnectorFactory

        if not ConnectorFactory.is_supported_type(src_type):
            supported_types = ConnectorFactory.get_supported_types()
            raise ValueError(
                f"Unsupported source type: '{src_type}'. "
                f"Supported types are: {', '.join(supported_types)}"
            )

    def __repr__(self) -> str:
        schedule_str = f"schedule='{self.schedule}'" if self.schedule else "no schedule"
        return f"Source(name='{self.name}', type='{self.src_type}', {schedule_str})"
