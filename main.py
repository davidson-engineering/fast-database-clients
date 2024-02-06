#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-11-19
# ---------------------------------------------------------------------------
"""Demonstration of how to use the FastInfluxDBClient class to send metrics to InfluxDB server"""
# ---------------------------------------------------------------------------

from fast_influxdb_client import FastInfluxDBClient
import random
import time
import logging
from datetime import datetime, timezone


def setup_logging(client):
    # get __main__ logger
    logger = logging.getLogger("fast_influxdb_client.fast_influxdb_client")

    # setup logging handler to console
    ch = logging.StreamHandler()
    # setup logging handler to file
    fh = logging.FileHandler("test.log")
    # setup logging handler to influxdb
    influx_handler = client.get_logging_handler()

    # set logging levels
    logger.setLevel(logging.DEBUG)
    ch.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)
    influx_handler.setLevel(logging.INFO)

    # setup logging format
    formatter = logging.Formatter(
        fmt="%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # setup logging to console
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    influx_handler.setFormatter(formatter)

    # add handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.addHandler(influx_handler)

    return logger


def main():
    """
    This is the main function that performs the following tasks:
    1. Creates a new client for connecting to InfluxDB.
    2. Sets the default bucket and creates the bucket if it doesn't exist.
    3. Sets up logging for the client.
    4. Updates the retention duration for the bucket.
    5. Generates random data and sends it to the InfluxDB server.
    """
    bucket = "metrics2"
    config_file = "config.toml"
    # Create new client
    with FastInfluxDBClient.from_config_file(config_file=config_file) as client:
        print(f"{client=}")
        client.default_bucket = bucket
        client.create_bucket(bucket)
        logger = setup_logging(client)
        client.update_bucket(bucket, retention_duration="10d")

        # client.query_table(f'from(bucket:"{bucket}") |> range(start: -1h)')

        # Generate some random data, and send to influxdb server
        while 1:
            data = random.random()
            data2 = random.randint(0, 100)
            data3 = random.choice([True, False])

            metrics = [
                dict(
                    measurement="py_metric1",
                    fields={"data1": data, "data2": data2, "data3": data3},
                    time=datetime.now(timezone.utc),
                )
                for _ in range(10_000)
            ]

            client.write_metric(metrics)
            time.sleep(1 + random.random())


if __name__ == "__main__":
    main()
