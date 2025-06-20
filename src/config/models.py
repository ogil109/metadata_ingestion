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
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON encoding")

        # Convert to list if single source to allow iteration
        if not isinstance(data, list):
            data = [data]

        # Build sources
        sources = [Source(**source) for source in data]
        return cls(os.path.basename(path), sources)


class Source:
    def __init__(self, name: str, type: str, schedule: str, connection: dict):
        # Validate required fields
        if not all(isinstance(x, str) for x in (name, type, schedule)):
            raise ValueError("name, type, and schedule must be strings")

        if type not in ("api", "db"):
            raise ValueError("type must be 'api' or 'db'")

        if not re.match(r"^\d+ \* \* \* \*$", schedule):
            raise ValueError("schedule must be in cron format (e.g., '0 * * * *')")

        self.name = name
        self.type = type
        self.schedule = schedule
        self.connection = connection
