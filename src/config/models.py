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
        # Validate required fields
        if src_type not in ("api", "odbc"):
            raise ValueError("src_type must be 'api' or 'odbc'")

        if schedule is not None and not re.match(r"^\d+ \* \* \* \*$", schedule):
            raise ValueError("schedule must be in cron format (e.g., '0 * * * *')")

        self.name = name
        self.src_type = src_type
        self.connection = connection
        self.schedule = schedule  # None means no scheduling, run once
