#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-03-01
# ---------------------------------------------------------------------------
"""FastInfluxDBClient is a class to enable rapid deployment of a client to send metrics to InfluxDB server"""

# ---------------------------------------------------------------------------
from dataclasses import dataclass, asdict, field
from datetime import datetime
from datetime import timezone
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

class InfluxDBWriteError(Exception):
    pass

class FastInfluxDBClientConfigError(Exception):
    pass

@dataclass
class InfluxMetric:
    measurement: str
    fields: dict
    time: datetime = field(default_factory=datetime.utcnow)
    tags: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        fields = [f"{k}:{v}" for k, v in self.fields.items()]
        return f"{self.__class__.__name__}: {self.measurement}-{fields} at {self.time}"

    def as_dict(self) -> dict:
        return asdict(self)


class InfluxDBLoggingHandler(logging.Handler):
    def __init__(self, client, bucket=None, org=None, measurement="logs", **kwargs):
        super().__init__(**kwargs)
        self._client = client
        self.measurement = measurement
        self.org = org or client.default_org
        self.log_bucket = bucket or client.default_bucket

    def emit(self, record):
        try:
            msg = self.format(record)
            fields = dict(
                level=record.levelno,
                msg=msg,
                name=record.name,
                path=record.pathname,
                lineno=record.lineno,
            )
            influx_metric = InfluxMetric(
                measurement=self.measurement, fields=fields, time=datetime.fromisoformat(record.asctime)
            )
            write_api = self._client.write_api()
            write_api.write(self.log_bucket, self.org, influx_metric, write_precision="s")
        except Exception:
            self.handleError(record)


class FastInfluxDBClient(InfluxDBClient):
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.
    """

    def __init__(self, url, token: str = None, default_bucket=None, debug=None, timeout=10_000, enable_gzip=False, org: str = None, default_tags: dict = None, **kwargs):
        self.default_org = org
        self.default_bucket = default_bucket
        super().__init__(url, token, debug, timeout, enable_gzip, org, default_tags, **kwargs)

    @classmethod
    def from_config_file(cls, config_file: str = "config.ini", debug:None = None, enable_gzip: bool = False, **kwargs):
        client = cls._from_config_file(config_file, debug, enable_gzip, **kwargs)
        client.default_org = client.org
        return client

    def write_metric(self, metric, write_option=SYNCHRONOUS, bucket=None, org=None):

        if bucket is None:
            if self.default_bucket is not None:
                bucket = self.default_bucket
            else:
                raise FastInfluxDBClientConfigError("No bucket or default bucket specified")
            
        if org is None:
            if self.default_org is not None:
                org = self.default_org
            else:
                raise FastInfluxDBClientConfigError("No org or default org specified") 
        
        try:
            # Check if fields contain invalid types before attempting to write
            for value in metric.fields.values():
                if not isinstance(value, (str, int, float, bool, datetime)):
                    raise ValueError(f"Invalid field type: {type(value)}")

            write_api = self.write_api(write_option)
            write_api.write(bucket, org, metric, write_precision="s")
        except Exception as e:
            logging.error(f"Failed to write data to InfluxDB: {e}")
            raise InfluxDBWriteError(f"Failed to write metric: {e}") from e

    def write_data(self, measurement: str, fields: dict, time=None):
        if time is None:
            time = datetime.now(timezone.utc)
        influx_metric = InfluxMetric(measurement=measurement, time=time, fields=fields)
        # Saving data to InfluxDB
        self.write_metric(influx_metric)

    def __repr__(self):
        return f"FastInfluxDBClient({self.default_org}, {self.default_bucket})"

    def get_logging_handler(self, datefmt="%Y-%m-%dT%H:%M:%S%z"):
        logging_handler = InfluxDBLoggingHandler(self)
        logging_formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s", datefmt=datefmt
        )
        logging_handler.setFormatter(logging_formatter)
        return logging_handler
