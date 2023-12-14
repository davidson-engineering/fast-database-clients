#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-03-01
# ---------------------------------------------------------------------------
"""FastInfluxDBClient is a class to enable rapid deployment of a client to send metrics to InfluxDB server"""

# ---------------------------------------------------------------------------
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Union
import re
import logging
import os
import sys

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from influxdb_client.domain.write_precision import WritePrecision

DEFAULT_WRITE_PRECISION = WritePrecision.MS

logger = logging.getLogger(__name__)


class ErrorException(Exception):
    """
    Base class for other exceptions
    """

    def __init__(self, message):
        self.message = message
        logger.error(message)


class FastInfluxDBClientConfigError(ErrorException):
    """
    Raised when there is an error with the config file
    """

    pass


SUCCESS = "SUCCESS"
FAILED = "FAILED"


@dataclass
class ActionOutcomeMessage:
    """
    A class to generate messages representing an action -> outcome
    """

    action: str
    action_verbose: str = None
    outcome: str = None

    @property
    def message(self):
        return f"{self.action} -> {self.outcome}"

    @property
    def message_verbose(self):
        return f"{self.action_verbose} -> {self.outcome}"

    def __call__(self, action=None, outcome=None, action_verbose=None):
        if action:
            self.action = action
        if outcome:
            self.outcome = outcome
        if action_verbose:
            self.action_verbose = action_verbose

        log_record = dict(msg=self.message, extra=dict(details=self.message_verbose))
        return log_record


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


class InfluxPacket:
    def __init__(
        self,
        measurement: str,
        fields=None,
        time: datetime = None,
        tags: dict = None,
    ):
        self.measurement = measurement
        self.fields = fields or {}
        self.tags = tags or {}

        if isinstance(time, datetime):
            self.time = time.astimezone(UTC)
        else:
            self.time = datetime.now(UTC)

    def __repr__(self) -> str:
        fields = [f"{k}:{v}" for k, v in self.fields.items()]
        return f"{self.__class__.__name__}: {self.measurement}-{fields} at {self.time}"

    def _asdict(self) -> dict:
        return self.__dict__


class InfluxMetric(InfluxPacket):
    """
    A class to represent an InfluxDB metric
    :param measurement: measurement name
    :param fields: dictionary of fields
    :param time: datetime object
    :param tags: dictionary of tags
    """

    pass


class InfluxLog(InfluxPacket):
    def __init__(self, record, msg, *args, measurement="logs", **kwargs):
        super().__init__(measurement=measurement, *args, **kwargs)

        try:
            details = record.details
        except AttributeError:
            details = ""

        self.fields = dict(
            level=record.levelno,
            msg=msg,
            name=record.name,
            path=record.pathname,
            lineno=record.lineno,
        )
        self.tags = dict(
            details=details,
            level=record.levelname,
            path=record.pathname,
            lineno=record.lineno,
            logger=record.name,
            function=f"{record.funcName}()",
            module=record.module,
            process=f"{record.processName}:{record.process}",
            thread=f"{record.threadName}:{record.thread}",
        )
        try:
            self.time = datetime.fromtimestamp(record.created).astimezone(UTC)
        except ValueError:
            date_format = "%Y-%m-%dT%H:%M:%S%z"
            self.time = datetime.strptime(record.asctime, date_format).astimezone(UTC)


class InfluxLoggingHandler(logging.Handler):
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
            influx_log = InfluxLog(
                measurement=self.measurement,
                msg=self.format(record),
                record=record,
            )

            with self._client.write_api() as write_api:
                write_api.write(
                    bucket=self.log_bucket,
                    org=self.org,
                    record=influx_log,
                    write_precision="ms",
                )

        except Exception:
            self.handleError(record)


class FastInfluxDBClient(InfluxDBClient):
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.
    """

    def __init__(
        self,
        url: str,
        token: str = None,
        default_bucket: str = None,
        debug=None,
        timeout: Union[int, tuple[int, int]] = 10_000,
        enable_gzip: bool = False,
        org: str = None,
        default_tags: dict = None,
        **kwargs,
    ):
        self.default_org = org
        self.default_bucket = default_bucket

        super().__init__(
            url, token, debug, timeout, enable_gzip, org, default_tags, **kwargs
        )
        if default_bucket is not None:
            self.create_bucket(default_bucket)

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
        # check if config file exisit
        if not os.path.exists(config_file):
            raise FastInfluxDBClientConfigError(
                "Config file '{config_file}' does not exist"
            )

        client = cls._from_config_file(config_file, debug, enable_gzip, **kwargs)
        client.default_org = client.org
        return client

    def write_metric(
        self,
        metric: Union[InfluxMetric, dict],
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

        log_action_outcome = ActionOutcomeMessage(
            action="Sending metric to influxdb",
            action_verbose=f"Sending metric:{metric.measurement} to bucket:'{bucket}' on influxdb at {self.url}",
        )

        try:
            # Check if fields contain invalid types before attempting to write
            for value in metric.fields.values():
                if not isinstance(value, (str, int, float, bool, datetime)):
                    raise ValueError(f"Invalid field type: {type(value)}")

            with self.write_api(write_options=write_option) as write_api:
                write_api.write(
                    bucket=bucket,
                    org=org,
                    record=metric,
                    write_precision=DEFAULT_WRITE_PRECISION,
                )
                logger.info(**log_action_outcome(outcome=SUCCESS))

        except InfluxDBError as e:
            logger.error(**log_action_outcome(outcome=FAILED))
            raise ErrorException(f"Failed to write metric: {e}") from e

    def write_data(self, measurement: str, fields: dict, time=None):
        """package some data into an InfluxMetric object, and send it to InfluxDB
        :param measurement: measurement name
        :param fields: dictionary of fields
        :param time: datetime object
        :return: None
        """
        if time is None:
            time = datetime.now(UTC)
        influx_metric = InfluxMetric(measurement=measurement, time=time, fields=fields)
        # Saving data to InfluxDB
        self.write_metric(influx_metric)

    def __repr__(self):
        return f"FastInfluxDBClient(url={self.url}, org={self.default_org}, default_bucket={self.default_bucket})"

    def get_logging_handler(self, datefmt="%Y%m%dT%H:%M:%S%z"):
        """Create a logging handler to send logs to InfluxDB
        :param datefmt: date format string
        :return: logging handler
        """
        logging_handler = InfluxLoggingHandler(self)
        logging_formatter = logging.Formatter(
            fmt="%(levelname)s %(message)s",
            datefmt=datefmt,
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
                logger.warning(
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

    def enable_verbose_logging_to_console(self):
        for _, logger in self.conf.loggers.items():
            logger.setLevel(logging.DEBUG)
            logger.addHandler(logging.StreamHandler(sys.stdout))


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
