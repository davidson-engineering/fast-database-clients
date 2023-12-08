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
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import toml
import pytz


class InfluxDBWriteError(Exception):
    pass


config = toml.load("config.toml")
TIMEZONE = pytz.timezone(config["system"]["timezone"])


class InfluxMetric:
    def __init__(self, measurement: str, fields: dict, time=None, tags=None):
        self.measurement: str = measurement
        self.fields: dict = fields
        self.time: datetime = time or datetime.utcnow()
        self.tags = tags or {}

    def __repr__(self) -> str:
        fields = [f"{k}:{v}" for k, v in self.fields.items()]
        return f"{self.measurement}-{fields} at {self.time}"


class InfluxDBLoggingHandler(logging.Handler):
    def __init__(self, client, bucket, measurement="logging", **kwargs):
        super().__init__(**kwargs)
        self.client = client
        self.bucket = bucket
        self.measurement = measurement

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
            self.client.write_data(
                measurement=self.measurement,
                time=datetime.fromtimestamp(record.created),
                fields=fields,
                bucket=self.bucket,
            )
        except Exception:
            self.handleError(record)


class FastInfluxDBClient(InfluxDBClient):
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.
    """

    def write_metric(self, metric, bucket, write_option=SYNCHRONOUS):
        try:
            # Check if fields contain invalid types before attempting to write
            for value in metric.fields.values():
                if not isinstance(value, (str, int, float, bool, datetime)):
                    msg = f"Invalid field type: {type(value)}"
                    logging.error(msg)
                    raise ValueError(msg)

            write_api = self.write_api(write_option)
            write_api.write(bucket, metric, write_precision="s")

            logging.debug(
                f"Sent metric: {metric} to bucket: {bucket} at {datetime.utcnow()}"
            )

        except Exception as e:
            msg = f"Failed to write metric: {e}"
            logging.error(msg)
            raise InfluxDBWriteError(msg) from e

    def write_data(self, measurement: str, fields: dict, bucket, time=None):
        if time is None:
            time = datetime.utcnow()
        if not isinstance(time, datetime):
            msg = f"An invalid time type was passed to write_data: {type(time)}"
            logging.error(msg)
            raise ValueError(msg)

        influx_metric = InfluxMetric(measurement=measurement, time=time, fields=fields)

        # Write data to InfluxDB
        self.write_metric(influx_metric, bucket=bucket)

    @classmethod
    def from_config_file(
        cls, config_file: str = "config.ini", debug=None, enable_gzip=False, **kwargs
    ):
        """
        Create a new FastInfluxDBClient from a config file.
        """
        return FastInfluxDBClient._from_config_file(
            config_file=config_file, debug=debug, enable_gzip=enable_gzip, **kwargs
        )

    def __repr__(self):
        return f"FastInfluxDBClient({self.url}, {self.org}, {self.token[:5]}...)"

    def get_logging_handler(self, bucket, datefmt="%Y-%m-%dT%H:%M:%S%z"):
        logging_handler = InfluxDBLoggingHandler(self, bucket)

        logging_formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s", datefmt=datefmt
        )
        logging_handler.setFormatter(logging_formatter)
        return logging_handler
