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
from datetime import datetime, timezone
from copy import deepcopy


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
    config_file = "config/influx_test.toml"
    # Create new client
    with FastInfluxDBClient.from_config_file(config_file=config_file) as client:
        print(f"{client=}")
        client.default_bucket = bucket
        client.create_bucket(bucket)

        # Generate some random data, and send to influxdb server
        data = random.random()
        data2 = random.randint(0, 100)
        data3 = random.choice([True, False])

        metrics = [
            dict(
                measurement="py_metric1",
                fields={"data1": data, "data2": data2, "data3": data3},
                time=datetime.now(),
            )
            for _ in range(10_000)
        ]
        client.buffer.extend(deepcopy(metrics))
        client.start()
        while True:
            print(len(client.buffer))
            time.sleep(1)
            client.buffer.extend(deepcopy(metrics))


if __name__ == "__main__":
    main()
