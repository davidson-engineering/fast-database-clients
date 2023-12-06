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
from dotenv import load_dotenv
import os
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import logging


class ClientEnvVariableNotDefined(Exception):
    pass


class InfluxDBWriteError(Exception):
    pass


@dataclass
class InfluxMetric:
    measurement: str
    fields: dict
    time: datetime = field(default_factory=datetime.utcnow)
    tags: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        fields = [f"{k}:{v}" for k, v in self.fields]
        return f"{self.__class__.__name__}: {self.measurement}-{fields} at {self.time}"

    def as_dict(self) -> dict:
        return asdict(self)


class InfluxDBLoggingHandler(logging.Handler):
    def __init__(self, client, bucket=None, org=None, measurement="logging", **kwargs):
        super().__init__(**kwargs)
        self.client = client
        self.bucket = bucket or client.bucket
        self.org = org or client.org
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
            influx_metric = InfluxMetric(
                measurement=self.measurement, fields=fields, time=record.created
            )
            write_api = self.client.write_api()
            write_api.write(self.bucket, self.org, influx_metric, write_precision="s")
        except Exception:
            self.handleError(record)


class FastInfluxDBClient:
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.
    """

    TOKEN_VAR = "TOKEN"
    CLIENT_URL_VAR = "CLIENT_URL"
    ORG_VAR = "ORG"
    BUCKET_VAR = "BUCKET"

    client = None

    def __init__(self, env_filepath=None):
        if env_filepath and not os.path.exists(env_filepath):
            raise ClientEnvVariableNotDefined(f"{env_filepath} does not exist")

        if env_filepath and os.path.exists(env_filepath):
            load_dotenv(env_filepath)
        else:
            load_dotenv()

        for var_name in [
            self.TOKEN_VAR,
            self.CLIENT_URL_VAR,
            self.ORG_VAR,
            self.BUCKET_VAR,
        ]:
            if os.getenv(var_name) is None:
                raise ClientEnvVariableNotDefined(
                    f"{var_name} is not defined in .env file"
                )

        token = os.getenv(self.TOKEN_VAR)
        url = os.getenv(self.CLIENT_URL_VAR)
        org = os.getenv(self.ORG_VAR)
        bucket = os.getenv(self.BUCKET_VAR)

        if self.client is None:
            self.client = InfluxDBClient(url=url, token=token)
        self.org = org
        self.bucket = bucket

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def write_metric(self, metric, write_option=SYNCHRONOUS, bucket=None, org=None):
        if bucket is None:
            bucket = self.bucket
        if org is None:
            org = self.org
        try:
            # Check if fields contain invalid types before attempting to write
            for value in metric.fields.values():
                if not isinstance(value, (str, int, float, bool, datetime)):
                    raise ValueError(f"Invalid field type: {type(value)}")

            write_api = self.client.write_api(write_option)
            write_api.write(bucket, org, metric, write_precision="s")
        except Exception as e:
            logging.error(f"Failed to write data to InfluxDB: {e}")
            raise InfluxDBWriteError(f"Failed to write metric: {e}") from e

    def write_data(self, measurement: str, fields: dict, time=None):
        if time is None:
            time = datetime.utcnow()
        influx_metric = InfluxMetric(measurement=measurement, time=time, fields=fields)
        # Saving data to InfluxDB
        self.write_metric(influx_metric)

    def __repr__(self):
        return f"FastInfluxDBClient({self.org}, {self.bucket})"

    def get_logging_handler(self, datefmt="%Y-%m-%dT%H:%M:%S%z"):
        logging_handler = InfluxDBLoggingHandler(self)
        logging_formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s", datefmt=datefmt
        )
        logging_handler.setFormatter(logging_formatter)
        return logging_handler
