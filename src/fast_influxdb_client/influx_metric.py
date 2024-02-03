#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Sequence


@dataclass
class InfluxMetric(Sequence):
    measurement: str
    fields: dict
    time: datetime = field(default_factory=datetime.utcnow)
    bucket: str = None
    tags: dict = None
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
        return f"{self.measurement}: {self.fields} @ {self.time}" + extra_info
