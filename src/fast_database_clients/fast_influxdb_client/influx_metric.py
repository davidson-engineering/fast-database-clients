#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Sequence
import time
from datetime import datetime
from typing import Union
import pytz

from fast_database_clients.exceptions import ErrorException


class TimeFormatError(ErrorException):
    def __init__(self, message="Time has not been provided in a compatible format"):
        super().__init__(message)

def dict_to_influx_metric(data: dict, defaults: dict = None) -> InfluxMetric:

    if isinstance(data, InfluxMetric):
        data = asdict(data)

    filtered_data = {key: value for key, value in data.items() if value is not None}

    # Apply default values if provided
    if defaults:
        for key, value in defaults.items():
            filtered_data.setdefault(key, value)

    return InfluxMetric(**filtered_data)

def convert_time_to_ns(time: datetime, write_precision:str='ns') -> int:
        if write_precision == "s": time_factor = 1
        elif write_precision == "ms": time_factor = 1e3
        elif write_precision == "us": time_factor = 1e6
        elif write_precision == "ns": time_factor = 1e9
        else: raise TimeFormatError(f"Write precision '{write_precision}' not supported")
        if isinstance(time, datetime):
            return int(time.timestamp() * time_factor)
        if isinstance(time, (int, float)):
            # Note this assumes time is in seconds
            return int(time * time_factor)
        else:
            raise TimeFormatError()

def localize_timestamp(timestamp_ns: int, timezone_str:str = "UTC") -> int:
    # Convert nanoseconds to seconds
    timestamp_sec = timestamp_ns / 1e9

    # Create a datetime object in the original timezone
    dt_original = datetime.utcfromtimestamp(timestamp_sec)
    original_timezone = pytz.timezone(timezone_str)
    dt_original = original_timezone.localize(dt_original)

    # Convert to UTC
    dt_utc = dt_original.astimezone(pytz.utc)

    # Convert the datetime back to timestamp in nanoseconds
    utc_timestamp_ns = int(dt_utc.timestamp() * 1e9)

    return utc_timestamp_ns


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
        setattr(self, "time", self.convert_time_to_ns(self.time, self.write_precision))

    def _convert_to_expected_type(self, value, expected_type):
        if expected_type is int and isinstance(value, (float, str)):
            return int(value)
        elif expected_type is float and isinstance(value, (int, str)):
            return float(value)
        elif expected_type is str and isinstance(value, (int, float)):
            return str(value)
        return value

        

