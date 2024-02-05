#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Sequence, Union


@dataclass
class InfluxMetric(Sequence):
    measurement: str
    fields: dict
    time: datetime = field(default_factory=datetime.utcnow)
    bucket: str = None
    tags: dict = field(default_factory=dict)
    priority: int = 0

    def __iter__(self) -> Any:
        yield from (
            self.measurement,
            self.time,
            self.fields,
            self.bucket,
            self.tags,
            self.priority,
        )

    def __getitem__(self, index) -> Any:
        if index == 0:
            return self.measurement
        elif index == 1:
            return self.time
        elif index == 2:
            return self.fields
        else:
            raise IndexError("InfluxMetric index out of range")

    def __len__(self) -> int:
        return len(asdict(self))

    def __repr__(self) -> str:
        process_extra = lambda x: f"{x[0]}: {x[1]}" if x[1] else ""
        extra_info = ", ".join(
            [
                process_extra(x)
                for x in [
                    ("tags", self.tags),
                    ("bucket", self.bucket),
                    ("priority", self.priority),
                ]
            ]
        )
        return f"{self.measurement}: {self.fields} @ {self.time}{extra_info}"

    def __post_init__(self):
        type_mapping = {'measurement': str, 'fields': dict, 'time': datetime, 'bucket': str, 'tags': dict, 'priority': int}
        for field_name, expected_type in type_mapping.items():
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, expected_type):
                raise TypeError(f"Expected {expected_type} for field '{field_name}', but got {type(value)}")

    def _convert_to_expected_type(self, value, expected_type):
        if expected_type is datetime and isinstance(value, (int, float)):
            return datetime.datetime.fromtimestamp(int(value), datetime.UTC) # Assumes all timestamps are UTC
        elif expected_type is int and isinstance(value, (float, str)):
            return int(value)
        elif expected_type is float and isinstance(value, (int, str)):
            return float(value)
        return value

def dict_to_influx_metric(data: dict, defaults: dict = None) -> InfluxMetric:

    if isinstance(data, InfluxMetric):
        data = asdict(data)

    filtered_data = {key: value for key, value in data.items() if value is not None}

    # Apply default values if provided
    if defaults:
        for key, value in defaults.items():
            data.setdefault(key, value)

    return InfluxMetric(**filtered_data)
