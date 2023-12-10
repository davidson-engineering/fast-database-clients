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
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
from influxdb_client.rest import ApiException


class InfluxDBWriteError(Exception):
    pass


class FastInfluxDBClientConfigError(Exception):
    pass


import re


def convert_to_seconds(time_string):
    unit_mapping = {"d": 24 * 60 * 60, "h": 60 * 60, "m": 60, "s": 1}

    # Use regular expression to match components like '1d', '30m', etc.
    matches = re.findall(r"(\d+)([dhms])", time_string)

    if matches:
        total_seconds = 0
        for match in matches:
            value, unit = int(match[0]), match[1]
            total_seconds += value * unit_mapping[unit]

        return total_seconds
    else:
        raise ValueError("Invalid time format")


class InfluxMetric:
    """
    A class to represent an InfluxDB metric
    :param measurement: measurement name
    :param fields: dictionary of fields
    :param time: datetime object
    :param tags: dictionary of tags
    """

    def __init__(
        self,
        measurement: str,
        fields=None,
        time: datetime = None,
        tags: dict = None,
    ) -> None:
        self.measurement = measurement
        self.fields = fields or {}
        self.time = time or datetime.now(timezone.utc)
        self.tags = tags or {}

    def __repr__(self) -> str:
        fields = [f"{k}:{v}" for k, v in self.fields.items()]
        return f"{self.__class__.__name__}: {self.measurement}-{fields} at {self.time}"

    def as_dict(self) -> dict:
        return asdict(self)


class InfluxDBLoggingHandler(logging.Handler):
    def __init__(
        self,
        client,
        name="influxdb_logger",
        bucket=None,
        org=None,
        measurement="logs",
        **kwargs,
    ):
        """
        Create a logging handler to send logs to InfluxDB
        :param name: name of logger
        :param client: FastInfluxDBClient object
        :param name: name of logger
        :param bucket: bucket name
        :param org: org name
        :param measurement: measurement name
        :param kwargs: additional keyword arguments
        """
        super().__init__(**kwargs)
        self.set_name(name)
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

            try:
                time = datetime.fromisoformat(record.asctime)
            except ValueError:
                date_format = "%Y-%m-%dT%H:%M:%S%z"
                time = datetime.strptime(record.asctime, date_format)

            influx_metric = InfluxMetric(
                measurement=self.measurement,
                fields=fields,
                time=time,
            )

            write_api = self._client.write_api()
            write_api.write(
                self.log_bucket, self.org, influx_metric, write_precision="s"
            )

        except Exception:
            self.handleError(record)


class FastInfluxDBClient(InfluxDBClient):
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.
    """

    def __init__(
        self,
        url,
        token: str = None,
        default_bucket=None,
        debug=None,
        timeout=10_000,
        enable_gzip=False,
        org: str = None,
        default_tags: dict = None,
        **kwargs,
    ):
        self.default_org = org
        self.default_bucket = default_bucket
        super().__init__(
            url, token, debug, timeout, enable_gzip, org, default_tags, **kwargs
        )

    @classmethod
    def from_config_file(
        cls,
        config_file: str = "config.ini",
        debug: None = None,
        enable_gzip: bool = False,
        **kwargs,
    ):
        """
        Create a new FastInfluxDBClient object from a config file
        :param config_file: path to config file
        :param debug: enable debug logging
        :param enable_gzip: enable gzip compression
        :param kwargs: additional keyword arguments
        :return: FastInfluxDBClient object
        """
        client = cls._from_config_file(config_file, debug, enable_gzip, **kwargs)
        client.default_org = client.org
        return client

    def write_metric(
        self,
        metric: InfluxMetric | dict,
        write_option=SYNCHRONOUS,
        bucket: str = None,
        org: str = None,
    ) -> None:
        """
        Write a metric to InfluxDB server
        :param metric: InfluxMetric object
        :param write_option: WriteOption object
        :param bucket: bucket name
        :param org: org name
        :return: None
        """
        if bucket is None:
            if self.default_bucket is not None:
                bucket = self.default_bucket
            else:
                raise FastInfluxDBClientConfigError(
                    "No bucket or default bucket specified"
                )

        if org is None:
            if self.default_org is not None:
                org = self.default_org
            else:
                raise FastInfluxDBClientConfigError("No org or default org specified")

        if isinstance(metric, dict):
            metric = InfluxMetric(**metric)

        try:
            # Check if fields contain invalid types before attempting to write
            for value in metric.fields.values():
                if not isinstance(value, (str, int, float, bool, datetime)):
                    raise ValueError(f"Invalid field type: {type(value)}")

            write_api = self.write_api(write_option)
            write_api.write(bucket, org, metric, write_precision="s")
            logging.debug(f"Sent metric: {metric} to {bucket} using client {self}")

        except Exception as e:
            logging.error(f"Failed to write data to InfluxDB: {e}")
            raise InfluxDBWriteError(f"Failed to write metric: {e}") from e

    def write_data(self, measurement: str, fields: dict, time=None):
        """package some data into an InfluxMetric object, and send it to InfluxDB
        :param measurement: measurement name
        :param fields: dictionary of fields
        :param time: datetime object
        :return: None
        """
        if time is None:
            time = datetime.now(timezone.utc)
        influx_metric = InfluxMetric(measurement=measurement, time=time, fields=fields)
        # Saving data to InfluxDB
        self.write_metric(influx_metric)

    def __repr__(self):
        return f"FastInfluxDBClient(url={self.url}, org={self.default_org}, default_bucket={self.default_bucket})"

    def get_logging_handler(self, datefmt="%Y-%m-%dT%H:%M:%S%z"):
        """Create a logging handler to send logs to InfluxDB
        :param datefmt: date format string
        :return: logging handler
        """
        logging_handler = InfluxDBLoggingHandler(self)
        logging_formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s", datefmt=datefmt
        )
        logging_handler.setFormatter(logging_formatter)
        return logging_handler

    def create_bucket(self, bucket_name: str, retention_duration: str = "30d"):
        """Create a bucket
        :param bucket_name: bucket name
        :param retention_policy: retention policy
        :return: None
        """
        retention_duration_secs = convert_to_seconds(retention_duration)
        try:
            self.buckets_api().create_bucket(
                bucket_name=bucket_name,
                retention_rules=[
                    {"type": "expire", "everySeconds": retention_duration_secs}
                ],
            )

        except ApiException as e:
            if e.status == 422:
                logging.warning(
                    f"Bucket {bucket_name} already exists. Bucket not created"
                )
            else:
                raise e

    def update_bucket(self, bucket_name: str, retention_duration: str = "30d"):
        """Update a bucket
        :param bucket_name: bucket name
        :param retention_policy: retention policy
        :return: None
        """
        # find bucket by name
        bucket = self.buckets_api().find_bucket_by_name(bucket_name=bucket_name)
        retention_duration_secs = convert_to_seconds(retention_duration)
        bucket.retention_rules = [
            {"type": "expire", "everySeconds": retention_duration_secs}
        ]
        self.buckets_api().update_bucket(bucket=bucket)

    def list_buckets(self):
        """List all buckets
        :return: list of buckets
        """
        return self.buckets_api().find_buckets()


def main():
    # create an fastinfluxDB client, and create a bucket
    bucket = "metrics3"
    config_file = "config.toml"
    # Create new client
    client = FastInfluxDBClient.from_config_file(config_file=config_file)
    print(f"{client=}")
    print(client.list_buckets())
    client.default_bucket = bucket
    client.create_bucket(bucket)
    client.update_bucket(bucket, retention_duration="7d")
    print(f"{client.default_bucket=}")


if __name__ == "__main__":
    main()
