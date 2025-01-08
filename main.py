#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-11-19
# ---------------------------------------------------------------------------
"""Demonstration of how to use the FastInfluxDBClient class to send metrics to InfluxDB server"""
# ---------------------------------------------------------------------------

from fast_database_clients import FastInfluxDBClient
import random
import time
import logging
from datetime import datetime


def setup_logging():
    # get __main__ logger
    logger = logging.getLogger()

    # setup logging handler to console
    ch = logging.StreamHandler()
    # setup logging handler to file
    fh = logging.FileHandler("test.log")
    # setup logging handler to influxdb

    # set logging levels
    logger.setLevel(logging.DEBUG)
    ch.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)

    # setup logging format
    formatter = logging.Formatter(
        fmt="%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # setup logging to console
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger


def main():
    logger = setup_logging()
    """
    This is the main function that performs the following tasks:
    1. Creates a new client for connecting to InfluxDB.
    2. Sets the default bucket and creates the bucket if it doesn't exist.
    3. Sets up logging for the client.
    4. Updates the retention duration for the bucket.
    5. Generates random data and sends it to the InfluxDB server.
    """
    bucket = "metrics2"
    config_file = "config/influx_config.toml"
    # Create new client
    with FastInfluxDBClient.from_config_file(config_file=config_file) as client:
        print(f"{client=}")
        client.default_bucket = bucket
        client.create_bucket(bucket)

        # Generate some random data, and send to influxdb server
        data = lambda: random.random()
        data2 = lambda: random.randint(0, 100)
        data3 = lambda: random.choice([True, False])

        def generate_metrics():
            while True:
                yield dict(
                    measurement="py_metric1",
                    fields={"data1": data(), "data2": data2(), "data3": data3()},
                    time=datetime.now(),
                )

        metrics = generate_metrics()

        client.start()

        client.buffer.extend([next(metrics) for _ in range(30_000)])

        while len(client.buffer) > 0:
            time.sleep(0.1)


if __name__ == "__main__":
    main()
