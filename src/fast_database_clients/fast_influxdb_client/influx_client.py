#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
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

Classes:
    FastInfluxDBClient: A class for sending data to an InfluxDB server using the InfluxDB client API.
    FastInfluxDBClientConfigError: An exception raised when there is an error with the config file.
    ErrorException: An exception raised when there is an error.
    ActionOutcomeMessage: A class to generate messages representing an action -> outcome.

Functions:

    convert_to_seconds: Convert a time string to seconds.
"""
# ---------------------------------------------------------------------------
from dataclasses import dataclass, asdict, is_dataclass
from typing import Iterable, Union, Tuple
import re
import logging
import os
import sys
from itertools import groupby
from operator import attrgetter
from typing import List
from datetime import datetime
import pytz

from influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from fast_database_clients.fast_database_client import DatabaseClientBase, load_config
from fast_database_clients.fast_influxdb_client.influx_metric import (
    InfluxMetric,
    convert_time,
)
from fast_database_clients.fast_influxdb_client.influx_logging import (
    InfluxLoggingHandler,
)
from fast_database_clients.action_outcome import ActionOutcome, ActionOutcomeMessage
from fast_database_clients.exceptions import ErrorException

DEFAULT_WRITE_PRECISION_DATA = "ns"
WRITE_BATCH_SIZE = 5000

logger = logging.getLogger(__name__)


class FastInfluxDBClientConfigError(ErrorException):
    """
    Raised when there is an error with the config file
    """

    pass


# def group_by_key(objects: List[dict], key) -> dict:
#     sorted_objects = sorted(objects, key=attrgetter(key))
#     grouped_objects = {
#         key: list(group) for key, group in groupby(sorted_objects, key=attrgetter(key))
#     }
#     return grouped_objects


def verify_write_precision(write_precision: str) -> bool:
    assert isinstance(write_precision, str)
    assert write_precision in set(("ns", "us", "ms", "s"))


def dict_to_point(
    data: Union[dict, InfluxMetric],
    write_precision=DEFAULT_WRITE_PRECISION_DATA,
    local_tz=None,
) -> Point:

    if not isinstance(data, dict):
        if is_dataclass(data):
            data = asdict(data)
        else:
            raise ValueError("data must be a dict or a dataclass")

    measurement = data.pop("measurement")
    time_value = data.pop("time")
    fields = data.pop("fields")

    if local_tz:
        time_value = localize_time(time_value, local_tz)

    time_value = convert_time(time_value, write_precision)

    point = Point(measurement).time(time_value, write_precision)

    for field_name, field_value in fields.items():
        point.field(field_name, field_value)

    if "tags" in data:
        for tag_name, tag_value in data.pop("tags").items():
            point.tag(tag_name, tag_value)

    return point


def localize_time(
    time: Union[int, float, datetime], timezone_str: str = "UTC"
) -> datetime:
    """
    Localize a timestamp to a timezone
    :param time: The timestamp to localize
    :param timezone_str: The timezone to localize to
    :return: The localized timestamp
    """

    if isinstance(time, (int, float)):
        dt_utc = datetime.fromtimestamp(time)
    elif isinstance(time, datetime):
        dt_utc = time
    else:
        raise ValueError("time must be a float, int, or datetime object")
    timezone = pytz.timezone(timezone_str)
    return timezone.localize(dt_utc)


# def chunks(lst, n):
#     """Yield successive n-sized chunks from lst."""
#     for i in range(0, len(lst), n):
#         yield lst[i : i + n]


def chunks(lst, target_length):
    """Yield successive chunks from lst based on the cumulative length of 'field' values."""
    chunk = []
    cumulative_length = 0

    for d in lst:
        field_length = len(d.get("field", ""))
        if cumulative_length + field_length > target_length:
            yield chunk
            chunk = []
            cumulative_length = 0

        chunk.append(d)
        cumulative_length += field_length

    if chunk:
        yield chunk


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


class BatchingCallback(object):
    """
    This class defines the callbacks for batched writes in the Fast InfluxDB Client.
    It provides methods for handling success, error, and retry scenarios.
    """

    def success(self, conf: (str, str, str), data: str):
        logger.debug(f"Written batch: {conf}, data: {data}")

    def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        logger.error(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        logger.warning(
            f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}"
        )


class FastInfluxDBClient(DatabaseClientBase):
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

    @classmethod
    def from_params(
        cls,
        url: str,
        token: str = None,
        default_bucket: str = None,
        debug=None,
        timeout: Union[int, Tuple[int, int]] = 10_000,
        enable_gzip: bool = False,
        org: str = None,
        default_tags: dict = None,
        default_write_precision: str = None,
        write_batch_size: int = WRITE_BATCH_SIZE,
        **kwargs,
    ):
        """
        Initialize a FastInfluxDBClient object from parameters.

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
        client = InfluxDBClient.__init__(
            url, token, debug, timeout, enable_gzip, org, default_tags, **kwargs
        )
        db_client = cls()
        db_client._client = client

        if default_write_precision:
            verify_write_precision(default_write_precision)
        db_client.default_write_precision = (
            default_write_precision or DEFAULT_WRITE_PRECISION_DATA
        )
        db_client.write_batch_size = write_batch_size
        if default_bucket is not None:
            db_client.create_bucket(default_bucket)
        return db_client

    @classmethod
    def from_config_file(
        cls,
        config_file: str = "config.toml",
        debug: None = None,
        enable_gzip: bool = False,
        buffer=None,
        write_interval=1,
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
        config = load_config(config_file).get("database_client")

        client = InfluxDBClient.from_config_file(
            config_file, debug, enable_gzip, **kwargs
        )
        db_client = cls(buffer=buffer, write_interval=write_interval)
        db_client._client = client
        db_client.write_batch_size = (
            config.get("influx").get("write_batch_size") or WRITE_BATCH_SIZE
        )
        default_write_precision = config.get("influx").get("default_write_precision")
        if default_write_precision:
            verify_write_precision(default_write_precision)
        db_client.default_write_precision = (
            default_write_precision or DEFAULT_WRITE_PRECISION_DATA
        )
        db_client.default_bucket = config.get("influx").get("default_bucket")
        db_client.local_tz = config.get("influx").get("local_tz", "UTC")
        return db_client

    def convert(
        self, metrics: Union[InfluxMetric, dict], write_precision: str = None
    ) -> Point:
        """
        Convert a container of metrics to a Point object.

        :param metrics: The metrics to convert.
        :return: The Point object.
        """
        if isinstance(metrics, (InfluxMetric, dict)):
            metrics = [metrics]

        write_precision = write_precision or self.write_precision

        metrics = [
            dict_to_point(
                metric, write_precision=write_precision, local_tz=self.local_tz
            )
            for metric in metrics
        ]

        return metrics

    def write(
        self,
        metrics: Union[
            InfluxMetric,
            dict,
            dataclass,
            Iterable[InfluxMetric],
            Iterable[dict],
            Iterable[dataclass],
        ],
        bucket: str = None,
        org: str = None,
        write_option=SYNCHRONOUS,
        write_precision: str = None,
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

        if org is None:
            if self.org is not None:
                org = self.org
            else:
                raise FastInfluxDBClientConfigError("No org specified")

        if write_precision is None:
            write_precision = self.default_write_precision
        else:
            assert verify_write_precision(write_precision)

        if bucket is None:
            if self.default_bucket is None:
                raise FastInfluxDBClientConfigError(
                    "No bucket specified, and no default bucket is specified."
                )
            bucket = self.default_bucket

        metrics = self.convert(metrics, write_precision=write_precision)

        with self._client.write_api(write_options=write_option) as write_api:
            metrics_chunks = chunks(metrics, self.write_batch_size)
            for metrics_batch in metrics_chunks:
                number_of_metrics = len(metrics_batch)
                log_action_outcome = ActionOutcomeMessage(
                    action=f"Sending {number_of_metrics} metrics to influxdb",
                    action_verbose=f"Sending {number_of_metrics} metrics to influxdb server at {self._client.url}",
                )
                outcome = ActionOutcome.SUCCESS
                try:
                    write_api.write(
                        bucket=bucket,
                        org=org,
                        record=metrics_batch,
                        write_precision=write_precision,
                    )
                except InfluxDBError as e:
                    outcome = ActionOutcome.FAILED
                    logger.error(
                        f"Failed to write metrics, some metrics will be discarded. {e}",
                        extra={"discarded_metrics": metrics_batch},
                    )
                finally:
                    logger.info(**log_action_outcome(outcome=outcome))

    def query_table(self, query: str):
        """
        In development

        """
        tables = self._client.query_api().query(query)

        for table in tables:
            print(table)
            for row in table.records:
                print(row.values)

    def query(self, query: str, org: str = None):
        """
        Query data from InfluxDB.

        :param query: The query string.
        :param org: The organization name.
        :return: The query result.
        """
        org = org or self.org
        return self._client.query_api().query(org=org, query=query)

    def __repr__(self):
        return f"FastInfluxDBClient(url={self._client.url}, org={self._client.org}, default_bucket={self.default_bucket})"

    def get_logging_handler(
        self,
        messagefmt="%(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ):
        """
        Create a logging handler to send logs to InfluxDB.

        :param datefmt: The date format string.
        :return: The logging handler.
        """
        return InfluxLoggingHandler(self, messagefmt=messagefmt, datefmt=datefmt)

    def create_bucket(self, bucket_name: str, retention_duration: str = "30d"):
        """
        Create a bucket.

        :param bucket_name: The bucket name.
        :param retention_policy: The retention policy.
        :return: None.
        """
        retention_duration_secs = convert_to_seconds(retention_duration)
        try:
            self._client.buckets_api().create_bucket(
                bucket_name=bucket_name,
                retention_rules=[
                    {"type": "expire", "everySeconds": retention_duration_secs}
                ],
            )

        except ApiException as e:
            if e.status == 422:
                logger.info(f"Bucket {bucket_name} already exists")
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
        bucket = self._client.buckets_api().find_bucket_by_name(bucket_name=bucket_name)
        retention_duration_secs = convert_to_seconds(retention_duration)
        bucket.retention_rules = [
            {"type": "expire", "everySeconds": retention_duration_secs}
        ]
        self._client.buckets_api().update_bucket(bucket=bucket)

    def list_buckets(self):
        """
        List all buckets.

        :return: List of buckets.
        """
        return self._client.buckets_api().find_buckets()

    def enable_verbose_logging_to_console(self):
        """
        Enable verbose logging to the console.

        This method sets the log level of all loggers in the configuration to DEBUG,
        and adds a StreamHandler to each logger to output log messages to the console.

        :return: None.
        """
        for _, logger in self._client.conf.loggers.items():
            logger.setLevel(logging.DEBUG)
            logger.addHandler(logging.StreamHandler(sys.stdout))

    def ping(self) -> bool:
        """
        Ping the InfluxDB server.

        :return: bool ping status.
        """
        return self._client.ping()

    @property
    def default_bucket(self):
        return self._default_bucket

    @default_bucket.setter
    def default_bucket(self, bucket: str):
        self._default_bucket = bucket
        # If the bucket does not exist, create it
        try:
            self.create_bucket(bucket)
        except InfluxDBError:
            logging.warning(
                "Provided token does not have sufficient permission to create buckets. This is non-critical."
            )

    @property
    def org(self):
        return self._client.org

    def write_api(self):
        return self._client.write_api()

    def close(self):
        self.__del__()

    def __del__(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def version(self):
        return self._client.version()


def main():
    from datetime import datetime

    # create an fastinfluxDB client, and create a bucket
    bucket = "metrics3"
    config_file = "config.toml"
    # Create new client
    client = FastInfluxDBClient.from_config_file(config_file=config_file)
    metric = InfluxMetric(
        measurement="test_measurement", fields={"value": 42}, time=datetime.now()
    )
    client.write(metric)


if __name__ == "__main__":
    main()
