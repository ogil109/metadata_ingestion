from __future__ import annotations

import json
import os
import re


class Pipeline:
    def __init__(
        self, name: str, config: dict, type: str, sources: list, sinks: list, schedule: str
    ):
        self.name = name
        self.config = config
        self.type = type
        self.sources = sources
        self.sinks = sinks
        self.schedule = schedule

    @classmethod
    def from_json(cls, path: str) -> Pipeline:
        try:
            with open(path) as f:
                data = json.load(f)
        except json.JSONDecodeError:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)

        # Validate JSON data
        if not all(key in data for key in ("type", "sources", "sinks", "schedule")):
            raise ValueError("Invalid JSON data, lacking required keys")

        if data["type"] not in ("batch", "stream"):
            raise ValueError("Invalid JSON data, type must be 'batch' or 'stream'")

        if data["sources"] not in ("api", "olap"):
            raise ValueError("Invalid JSON data, type must be 'api' or 'olap'")

        if not re.match(r"^\d+ \* \* \* \*$", data["schedule"]):
            raise ValueError("Invalid JSON data, schedule must be in cron format")

        return cls(
            os.path.basename(path),
            data,
            data["type"],
            data["sources"],
            data["sinks"],
            data["schedule"],
        )
