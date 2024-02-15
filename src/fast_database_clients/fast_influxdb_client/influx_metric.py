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


def convert_time(time: Union[datetime, int, float], write_precision: str = "ns") -> int:
    """
    Convert time to a specified precision

    Args:
        time (Union[datetime, int, float]): The time in seconds, datetime or timestamp
        write_precision (str): The precision of the output time. Can be 's', 'ms', 'us', 'ns'
    Returns:
        int: The time in the specified precision
    """
    if write_precision == "s":
        time_factor = 1
    elif write_precision == "ms":
        time_factor = 1e3
    elif write_precision == "us":
        time_factor = 1e6
    elif write_precision == "ns":
        time_factor = 1e9
    else:
        raise TimeFormatError(f"Write precision '{write_precision}' not supported")
    if isinstance(time, datetime):
        return int(time.timestamp() * time_factor)
    if isinstance(time, (int, float)):
        # Note this assumes time is in seconds
        return int(time * time_factor)
    else:
        raise TimeFormatError()


def check_attributes(
    metric: dict, keys: tuple = ("measurement", "fields", "time")
) -> bool:
    try:
        assert all(key in metric for key in keys)
    except AssertionError as e:
        raise AttributeError(f"Metric must contain {keys}") from e
    return True


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
        if isinstance(index, str):
            # Replicate dict like key:value getitem behaviour
            return asdict(self)[index]
        if index == 0:
            return self.measurement
        elif index == 1:
            return self.fields
        elif index == 2:
            return self.time
        elif index == 3:
            return self.tags
        else:
            raise IndexError(f"InfluxMetric index '{index}' out of range")

    def __setitem__(self, key, value):
        if key in [field.name for field in fields(self)]:
            setattr(self, key, value)
        else:
            raise KeyError(f"'{type(self).__name__}' object has no attribute '{key}'")

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
        # Check that all required fields are present
        check_attributes(asdict(self))
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
        # Convert time to specified precision
        # setattr(self, "time", convert_time(self.time, self.write_precision))

    def _convert_to_expected_type(self, value, expected_type):
        if expected_type is int and isinstance(value, (float, str)):
            return int(value)
        elif expected_type is float and isinstance(value, (int, str)):
            return float(value)
        elif expected_type is str and isinstance(value, (int, float)):
            return str(value)
        return value
