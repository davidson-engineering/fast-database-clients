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


@dataclass
class InfluxMetric(Sequence):
    measurement: str
    fields: dict = field(default_factory=dict)
    time: float = field(default_factory=time.time)
    tags: dict = field(default_factory=dict)

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
            "time": float,
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

    def _convert_to_expected_type(self, value, expected_type):
        if expected_type is int and isinstance(value, (float, str)):
            return int(value)
        elif expected_type is float and isinstance(value, (int, str)):
            return float(value)
        elif expected_type is str and isinstance(value, (int, float)):
            return str(value)
        elif expected_type is float and isinstance(value, datetime):
            return value.timestamp()
        return value


def dict_to_influx_metric(data: dict, defaults: dict = None) -> InfluxMetric:

    if isinstance(data, InfluxMetric):
        data = asdict(data)

    filtered_data = {key: value for key, value in data.items() if value is not None}

    # Apply default values if provided
    if defaults:
        for key, value in defaults.items():
            filtered_data.setdefault(key, value)

    return InfluxMetric(**filtered_data)
