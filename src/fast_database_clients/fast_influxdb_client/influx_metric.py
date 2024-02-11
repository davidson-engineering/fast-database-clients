#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from dataclasses import dataclass, field, asdict
from typing import Any, Sequence
import time
from datetime import datetime
from fast_database_clients.exceptions import ErrorException

class TimeFormatError(ErrorException):
    def __init__(self, message="Time has not been provided in a compatible format"):
        super().__init__(message)

@dataclass
class InfluxMetric(Sequence):
    measurement: str
    fields: dict = field(default_factory=dict)
    time: float = field(default_factory=time.time_ns)
    tags: dict = field(default_factory=dict)
    write_precision: str = "ns"

    def __iter__(self) -> Any:
        yield from (
            self.measurement,
            self.time,
            self.fields,
            self.tags,
        )

    def __getitem__(self, index) -> Any:
        if index == 0:
            return self.measurement
        elif index == 1:
            return self.fields
        elif index == 2:
            return self.time
        elif index == 3:
            return self.tags
        else:
            raise IndexError("InfluxMetric index out of range")

    def __len__(self) -> int:
        return len(asdict(self))

    def __repr__(self) -> str:
        return f"{self.measurement}: {self.fields} @ {self.time} | {self.tags}"

    def __post_init__(self):
        type_mapping = {
            "measurement": str,
            "fields": dict,
            "tags": dict,
        }
        for field_name, expected_type in type_mapping.items():
            value = getattr(self, field_name)
            try:
                if value is not None and not isinstance(value, expected_type):
                    raise TypeError(
                        f"Expected {expected_type} for field '{field_name}', but got {type(value)}"
                    )
            except TypeError:
                converted_value = self._convert_to_expected_type(value, expected_type)
                setattr(self, field_name, converted_value)
        # Convert time to ns
        setattr(self, "time", self._convert_time_to_ns(self.time))

    def _convert_to_expected_type(self, value, expected_type):
        if expected_type is int and isinstance(value, (float, str)):
            return int(value)
        elif expected_type is float and isinstance(value, (int, str)):
            return float(value)
        elif expected_type is str and isinstance(value, (int, float)):
            return str(value)
        return value

    def _convert_time_to_ns(self, time: datetime) -> int:
        wp = self.write_precision
        if wp == "s": time_factor = 1
        elif wp == "ms": time_factor = 1e3
        elif wp == "us": time_factor = 1e6
        elif wp == "ns": time_factor = 1e9
        else: raise TimeFormatError(f"Write precision '{wp}' not supported")
        if isinstance(time, datetime):
            return int(time.timestamp() * time_factor)
        if isinstance(time, (int, float)):
            return int(time * time_factor)
        else:
            raise TimeFormatError()

def dict_to_influx_metric(data: dict, defaults: dict = None) -> InfluxMetric:

    if isinstance(data, InfluxMetric):
        data = asdict(data)

    filtered_data = {key: value for key, value in data.items() if value is not None}

    # Apply default values if provided
    if defaults:
        for key, value in defaults.items():
            filtered_data.setdefault(key, value)

    return InfluxMetric(**filtered_data)
