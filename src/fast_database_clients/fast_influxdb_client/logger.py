#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from datetime import datetime, timezone
import logging

from influxdb_client.domain.write_precision import WritePrecision

from fast_database_clients.fast_influxdb_client.metric import InfluxMetric

DEFAULT_WRITE_PRECISION_LOGS = WritePrecision.MS


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
            record_time=f"{record.asctime},{int(record.msecs)}",
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
        name="influxdb_logger",
        bucket=None,
        org=None,
        measurement="logs",
        time_precision=DEFAULT_WRITE_PRECISION_LOGS,
        messagefmt="%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",  # ISO8601 format
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
        super().__init__(**kwargs)
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
