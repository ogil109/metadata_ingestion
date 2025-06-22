from __future__ import annotations

import json
import os
import re
from pathlib import Path


class Pipeline:
    def __init__(self, name: str, sources: list[Source]):
        self.name = name
        self.sources = sources

    @classmethod
    def from_json(cls, path: Path) -> Pipeline:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON encoding: {str(e)}") from e

        # Convert to list if single source to allow iteration
        if not isinstance(data, list):
            data = [data]

        # Build sources
        sources = [Source(**source) for source in data]
        return cls(os.path.basename(path), sources)


class Source:
    def __init__(self, name: str, src_type: str, connection: dict, schedule: str | None = None):
        # Validate src_type by checking if a corresponding connector exists
        self._validate_src_type(src_type)

        # Validate schedule format if provided
        if schedule is not None and not re.match(r"^\d+ \* \* \* \*$", schedule):
            raise ValueError("schedule must be in cron format (e.g., '0 * * * *')")

        self.name = name
        self.src_type = src_type
        self.connection = connection
        self.schedule = schedule  # None means no scheduling, run once

    def _validate_src_type(self, src_type: str) -> None:
        """Validate that the src_type has a corresponding connector class.

        Args:
            src_type: The source type to validate

        Raises:
            ValueError: If the src_type is not supported
        """
        # Import here to avoid circular imports
        from src.connectors.factory import ConnectorFactory

        if not ConnectorFactory.is_supported_type(src_type):
            supported_types = ConnectorFactory.get_supported_types()
            raise ValueError(
                f"Unsupported source type: '{src_type}'. "
                f"Supported types are: {', '.join(supported_types)}"
            )
