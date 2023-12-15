#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-03-01
# ---------------------------------------------------------------------------
"""
FastInfluxDBClient is a class to enable rapid deployment of a client to send metrics to InfluxDB server

This module contains the FastInfluxDBClient class, which is a subclass of the InfluxDBClient class from the
influxdb-client-python package. The FastInfluxDBClient class provides a simple interface for sending metrics to
an InfluxDB server.

Example:
    >>> from fast_influxdb_client import FastInfluxDBClient
    >>> client = FastInfluxDBClient.from_config_file(config_file="config.toml")
    >>> client.write_data(measurement="test_measurement", fields={"value": 42})

Attributes:
    DEFAULT_WRITE_PRECISION_DATA (str): The default write precision for metrics.
    DEFAULT_WRITE_PRECISION_LOGS (str): The default write precision for logs.

Classes:
    FastInfluxDBClient: A class for sending data to an InfluxDB server using the InfluxDB client API.
    InfluxMetric: A class to represent an InfluxDB metric.
    InfluxLoggingHandler: A class to send logs to InfluxDB.
    FastInfluxDBClientConfigError: An exception raised when there is an error with the config file.
    ErrorException: An exception raised when there is an error.
    ActionOutcomeMessage: A class to generate messages representing an action -> outcome.

Functions:

    convert_to_seconds: Convert a time string to seconds.
"""
# ---------------------------------------------------------------------------
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Union, Tuple
import re
import logging
import os
import sys
from enum import Enum

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from influxdb_client.domain.write_precision import WritePrecision

DEFAULT_WRITE_PRECISION_DATA = WritePrecision.S
DEFAULT_WRITE_PRECISION_LOGS = WritePrecision.MS


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


# Enum type for success and failure of action->outcome messaging pair
class ActionOutcome(Enum):
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class ActionOutcomeMessage:
    """
    A class to generate messages representing an action -> outcome

    Attributes:
        action (str): The action.
        action_verbose (str): The verbose action.
        outcome (str): The outcome.

    Methods:
        message: Get the message representing the action -> outcome.
        message_verbose: Get the verbose message representing the action -> outcome.
        __call__: Update the action, outcome, or action_verbose attributes and return a log record.

    Examples:
        >>> log_action_outcome = ActionOutcomeMessage(
                action="Sending metric to influxdb",
                action_verbose=f"Sending metric:{metric.name} to bucket:'{bucket}' on influxdb at {self.url}"
            )
        >>> log_action_outcome(outcome=ActionOutcome.SUCCESS)
        {
            'msg': 'Sending metric to influxdb -> success',
            'extra': {'details': "Sending metric:py_metric1 to bucket:'metrics2' on influxdb at http://localhost:8086"}
        }
        >>> log_action_outcome(outcome=ActionOutcome.FAILED)
        {
            'msg': 'Sending metric to influxdb -> failed',
            'extra': {'details': "Sending metric:py_metric1 to bucket:'metrics2' on influxdb at http://localhost:8086"}
        }
    """

    action: str
    action_verbose: str = None
    outcome: str = None

    @property
    def message(self):
        """
        Get the message representing the action -> outcome.

        Returns:
            str: The message representing the action -> outcome.
        """
        return f"{self.action} -> {self.outcome}"

    @property
    def message_verbose(self):
        """
        Get the verbose message representing the action -> outcome.

        Returns:
            str: The verbose message representing the action -> outcome.
        """
        return f"{self.action_verbose} -> {self.outcome}"

    def __call__(self, action=None, outcome=None, action_verbose=None):
        """
        Update the action, outcome, or action_verbose attributes and return a log record.

        Args:
            action (str): The action to update.
            outcome (str): The outcome to update.
            action_verbose (str): The verbose action to update.

        Returns:
            dict: A log record containing the message and details.
        """
        if action:
            self.action = action
        if outcome:
            self.outcome = outcome
        if action_verbose:
            self.action_verbose = action_verbose

        log_record = dict(msg=self.message, extra=dict(details=self.message_verbose))
        return log_record


def convert_to_seconds(time_string):
    """
    Convert a time string to seconds

    :param time_string: A string representing a time duration. The string can contain multiple components
                        separated by spaces, where each component consists of a number followed by a unit.
                        Valid units are 'd' for days, 'h' for hours, 'm' for minutes, and 's' for seconds.
                        Examples of valid time strings: "1d", "1h", "1m", "1s", "1d1h1m1s", "1d 1h 1m 1s".

    :return: The total number of seconds represented by the time string.

    :raises ValueError: If the time string is invalid or does not match the expected format.

    Examples:
    >>> convert_to_seconds("1d")
    86400
    >>> convert_to_seconds("1h")
    3600
    >>> convert_to_seconds("1m")
    60
    >>> convert_to_seconds("1s")
    1
    >>> convert_to_seconds("1d1h1m1s")
    90061
    >>> convert_to_seconds("1d 1h 1m 1s")
    90061
    """
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
    A class to represent an InfluxDB metric.

    Attributes:
        measurement (str): The measurement name of the metric.
        fields (dict): The fields of the metric.
        time (datetime): The timestamp of the metric.
        tags (dict): The tags associated with the metric.
    """

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
            self.time = time.astimezone(timezone.utc)
        else:
            self.time = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        fields = [f"{k}:{v}" for k, v in self.fields.items()]
        return f"{self.__class__.__name__}: {self.measurement}-{fields} at {self.time}"

    def _asdict(self) -> dict:
        return self.__dict__


class InfluxLog(InfluxMetric):
    """
    A class to represent an InfluxDB log

    Attributes:
        fields (dict): A dictionary containing the log fields.
        tags (dict): A dictionary containing the log tags.
        time (datetime): The timestamp of the log entry.
    """

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
        self.time = datetime.fromtimestamp(record.created).astimezone(timezone.utc)


class InfluxLoggingHandler(logging.Handler):
    """
    A class to send logs to InfluxDB

    Args:
        client (FastInfluxDBClient): FastInfluxDBClient object
        name (str, optional): Name of the logger. Defaults to "influxdb_logger".
        bucket (str, optional): Bucket name. Defaults to None.
        org (str, optional): Organization name. Defaults to None.
        measurement (str, optional): Measurement name. Defaults to "logs".
        time_precision (str, optional): Time precision. Defaults to DEFAULT_WRITE_PRECISION_LOGS.
        messagefmt (str, optional): Message format string. Defaults to "%(levelname)s %(message)s".
        datefmt (str, optional): Date format string. Defaults to "%Y%m%dT%H:%M:%S%z".
        args: Additional arguments.
        kwargs: Additional keyword arguments.

    Attributes:
        _client (FastInfluxDBClient): FastInfluxDBClient object
        measurement (str): Measurement name.
        org (str): Organization name.
        log_bucket (str): Bucket name.
        time_precision (str): Time precision.
        formatter (logging.Formatter): The logging formatter.

    Methods:
        emit: Emits a log record to InfluxDB.
        __init__: Create a logging handler to send logs to InfluxDB.

    Examples:
        >>> from fast_influxdb_client import FastInfluxDBClient, InfluxLoggingHandler
        >>> client = FastInfluxDBClient.from_config_file(config_file="config.toml")
        >>> handler = InfluxLoggingHandler(client)
        >>> logger = logging.getLogger("test_logger")
        >>> logger.addHandler(handler)
        >>> logger.setLevel(logging.INFO)
        >>> logger.info("This is a test log message")
    """

    def __init__(
        self,
        client,
        *args,
        name="influxdb_logger",
        bucket=None,
        org=None,
        measurement="logs",
        time_precision=DEFAULT_WRITE_PRECISION_LOGS,
        messagefmt="%(levelname)s %(message)s",
        datefmt="%Y%m%dT%H:%M:%S%z",
        **kwargs,
    ):
        """
        Create a logging handler to send logs to InfluxDB

        Args:
            client (FastInfluxDBClient): FastInfluxDBClient object
            name (str, optional): Name of the logger. Defaults to "influxdb_logger".
            bucket (str, optional): Bucket name. Defaults to None.
            org (str, optional): Organization name. Defaults to None.
            measurement (str, optional): Measurement name. Defaults to "logs".
            time_precision (str, optional): Time precision. Defaults to DEFAULT_WRITE_PRECISION_LOGS.
            messagefmt (str, optional): Message format string. Defaults to "%(levelname)s %(message)s".
            datefmt (str, optional): Date format string. Defaults to "%Y%m%dT%H:%M:%S%z".
            args: Additional arguments.
            kwargs: Additional keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.set_name(name)
        self._client = client
        self.measurement = measurement
        self.org = org or client.default_org
        self.log_bucket = bucket or client.default_bucket
        self.time_precsion = time_precision
        self.setFormatter(logging.Formatter(fmt=messagefmt, datefmt=datefmt))

    def emit(self, record):
        """
        Emits a log record to InfluxDB.

        Args:
            record (logging.LogRecord): The log record to emit.

        Raises:
            Exception: If an error occurs while writing to InfluxDB.
        """
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
                    write_precision=self.time_precsion,
                )

        except Exception:
            self.handleError(record)


class FastInfluxDBClient(InfluxDBClient):
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.

    Args:
        url (str): The URL of the InfluxDB server.
        token (str, optional): The authentication token. Defaults to None.
        default_bucket (str, optional): The default bucket name. Defaults to None.
        debug (bool, optional): Enable debug logging. Defaults to None.
        timeout (int or tuple[int, int], optional): The timeout value in milliseconds. Defaults to 10_000.
        enable_gzip (bool, optional): Enable gzip compression. Defaults to False.
        org (str, optional): The organization name. Defaults to None.
        default_tags (dict, optional): The default tags to include with each write. Defaults to None.
        default_write_precision (str, optional): The default write precision. Defaults to DEFAULT_WRITE_PRECISION_DATA.
        kwargs: Additional keyword arguments to pass to the base class constructor.

    Attributes:
        default_bucket (str): The default bucket name.
        default_write_precision (str): The default write precision.

    Methods:
        from_config_file: Create a new FastInfluxDBClient object from a config file.
        write_metric: Write a metric to InfluxDB server.
        write_data: Package some data into an InfluxMetric object and send it to InfluxDB.
        get_logging_handler: Create a logging handler to send logs to InfluxDB.
        create_bucket: Create a bucket.
        update_bucket: Update a bucket.
        list_buckets: List all buckets.
        enable_verbose_logging_to_console: Enable verbose logging to console.
    """

    # Rest of the code...


class FastInfluxDBClient(InfluxDBClient):
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.

    Args:
        url (str): The URL of the InfluxDB server.
        token (str, optional): The authentication token for the InfluxDB server. Defaults to None.
        default_bucket (str, optional): The default bucket to write data to. Defaults to None.
        debug (bool, optional): Enable debug logging. Defaults to None.
        timeout (int or tuple[int, int], optional): The timeout for requests to the InfluxDB server. Defaults to 10_000.
        enable_gzip (bool, optional): Enable gzip compression for requests. Defaults to False.
        org (str, optional): The organization name for the InfluxDB server. Defaults to None.
        default_tags (dict, optional): The default tags to include with each metric. Defaults to None.
        default_write_precision (str, optional): The default write precision for metrics. Defaults to DEFAULT_WRITE_PRECISION_DATA.

    Attributes:
        default_bucket (str): The default bucket to write data to.
        default_write_precision (str): The default write precision for metrics.

    Methods:
        from_config_file: Create a new FastInfluxDBClient object from a config file.
        write_metric: Write a metric to the InfluxDB server.
        write_data: Package some data into an InfluxMetric object and send it to InfluxDB.
        get_logging_handler: Create a logging handler to send logs to InfluxDB.
        create_bucket: Create a bucket.
        update_bucket: Update a bucket.
        list_buckets: List all buckets.
        enable_verbose_logging_to_console: Enable verbose logging to the console.
    """

    def __init__(
        self,
        url: str,
        *args,
        token: str = None,
        default_bucket: str = None,
        debug=None,
        timeout: Union[int, Tuple[int, int]] = 10_000,
        enable_gzip: bool = False,
        org: str = None,
        default_tags: dict = None,
        default_write_precision=DEFAULT_WRITE_PRECISION_DATA,
        **kwargs,
    ):
        """
        Initialize a FastInfluxDBClient object.

        Args:
            url (str): The URL of the InfluxDB server.
            token (str, optional): The authentication token for the InfluxDB server. Defaults to None.
            default_bucket (str, optional): The default bucket to write data to. Defaults to None.
            debug (bool, optional): Enable debug logging. Defaults to None.
            timeout (int or tuple[int, int], optional): The timeout for requests to the InfluxDB server. Defaults to 10_000.
            enable_gzip (bool, optional): Enable gzip compression for requests. Defaults to False.
            org (str, optional): The organization name for the InfluxDB server. Defaults to None.
            default_tags (dict, optional): The default tags to include with each metric. Defaults to None.
            default_write_precision (str, optional): The default write precision for metrics. Defaults to DEFAULT_WRITE_PRECISION_DATA.
            kwargs: Additional keyword arguments.
        """
        self.default_bucket = default_bucket
        self.default_write_precision = default_write_precision

        super().__init__(
            url, token, debug, timeout, enable_gzip, org, default_tags, *args, **kwargs
        )
        if default_bucket is not None:
            self.create_bucket(default_bucket)


class FastInfluxDBClient(InfluxDBClient):
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.
    """

    def __init__(
        self,
        url: str,
        *args,
        token: str = None,
        default_bucket: str = None,
        debug=None,
        timeout: Union[int, Tuple[int, int]] = 10_000,
        enable_gzip: bool = False,
        org: str = None,
        default_tags: dict = None,
        default_write_precision=DEFAULT_WRITE_PRECISION_DATA,
        **kwargs,
    ):
        """
        Initialize a FastInfluxDBClient object.

        :param url: The URL of the InfluxDB server.
        :param token: The authentication token for the InfluxDB server.
        :param default_bucket: The default bucket to write data to.
        :param debug: Enable debug logging.
        :param timeout: The timeout for requests to the InfluxDB server.
        :param enable_gzip: Enable gzip compression for requests.
        :param org: The organization name for the InfluxDB server.
        :param default_tags: The default tags to include with each metric.
        :param default_write_precision: The default write precision for metrics.
        :param kwargs: Additional keyword arguments.
        """
        self.default_bucket = default_bucket
        self.default_write_precision = default_write_precision

        super().__init__(
            url, token, debug, timeout, enable_gzip, org, default_tags, *args, **kwargs
        )
        if default_bucket is not None:
            self.create_bucket(default_bucket)

    @classmethod
    def from_config_file(
        cls,
        *args,
        config_file: str = "config.ini",
        debug: None = None,
        enable_gzip: bool = False,
        **kwargs,
    ):
        """
        Create a new FastInfluxDBClient object from a config file.

        :param config_file: The path to the config file.
        :param debug: Enable debug logging.
        :param enable_gzip: Enable gzip compression for requests.
        :param kwargs: Additional keyword arguments.
        :return: FastInfluxDBClient object.
        """
        # check if config file exists
        if not os.path.exists(config_file):
            raise FastInfluxDBClientConfigError(
                f"Config file '{config_file}' does not exist"
            )

        client = cls._from_config_file(config_file, debug, enable_gzip, *args, **kwargs)
        client.default_org = client.org
        return client

    def write_metric(
        self,
        metric: Union[InfluxMetric, dict],
        write_option=SYNCHRONOUS,
        bucket: str = None,
        org: str = None,
        write_precision=None,
    ) -> None:
        """
        Write a metric to the InfluxDB server.

        :param metric: The metric to write.
        :param write_option: The write option for the metric.
        :param bucket: The bucket to write the metric to.
        :param org: The organization for the metric.
        :param write_precision: The write precision for the metric.
        :return: None.
        """
        if bucket is None:
            if self.default_bucket is not None:
                bucket = self.default_bucket
            else:
                raise FastInfluxDBClientConfigError(
                    "No bucket or default bucket specified"
                )

        if org is None:
            if self.org is not None:
                org = self.org
            else:
                raise FastInfluxDBClientConfigError("No org specified")

        if write_precision is None:
            write_precision = self.default_write_precision

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
                    write_precision=write_precision,
                )
                logger.info(**log_action_outcome(outcome=ActionOutcome.SUCCESS))

        except InfluxDBError as e:
            logger.error(**log_action_outcome(outcome=ActionOutcome.FAILED))
            raise ErrorException(f"Failed to write metric: {e}") from e

    def write_data(
        self,
        measurement: str,
        fields: dict,
        time=None,
        write_precision=None,
    ):
        """
        Package some data into an InfluxMetric object and send it to InfluxDB.

        :param measurement: The measurement name.
        :param fields: The dictionary of fields.
        :param time: The datetime object.
        :return: None.
        """
        write_precision = write_precision or self.default_write_precision
        time = time or datetime.now(timezone.utc)

        influx_metric = InfluxMetric(
            measurement=measurement,
            time=time,
            fields=fields,
            write_precision=write_precision,
        )
        # Saving data to InfluxDB
        self.write_metric(influx_metric)

    def __repr__(self):
        return f"FastInfluxDBClient(url={self.url}, org={self.default_org}, default_bucket={self.default_bucket})"

    def get_logging_handler(
        self, messagefmt="%(levelname)s %(message)s", datefmt="%Y%m%dT%H:%M:%S%z"
    ):
        """
        Create a logging handler to send logs to InfluxDB.

        :param datefmt: The date format string.
        :return: The logging handler.
        """
        return InfluxLoggingHandler(self, fmt=messagefmt, datefmt=datefmt)

    def create_bucket(self, bucket_name: str, retention_duration: str = "30d"):
        """
        Create a bucket.

        :param bucket_name: The bucket name.
        :param retention_policy: The retention policy.
        :return: None.
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
        """
        Update a bucket.

        :param bucket_name: The bucket name.
        :param retention_policy: The retention policy.
        :return: None.
        """
        # find bucket by name
        bucket = self.buckets_api().find_bucket_by_name(bucket_name=bucket_name)
        retention_duration_secs = convert_to_seconds(retention_duration)
        bucket.retention_rules = [
            {"type": "expire", "everySeconds": retention_duration_secs}
        ]
        self.buckets_api().update_bucket(bucket=bucket)

    def list_buckets(self):
        """
        List all buckets.

        :return: List of buckets.
        """
        return self.buckets_api().find_buckets()

    def enable_verbose_logging_to_console(self):
        """
        Enable verbose logging to the console.

        This method sets the log level of all loggers in the configuration to DEBUG,
        and adds a StreamHandler to each logger to output log messages to the console.

        :return: None.
        """
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
