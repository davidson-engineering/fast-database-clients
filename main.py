#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-11-19
# version ='1.0'
# ---------------------------------------------------------------------------
"""Demonstration of how to use the FastInfluxDBClient class to send metrics to InfluxDB server"""
# ---------------------------------------------------------------------------

from fast_influxdb_client import FastInfluxDBClient, InfluxMetric
import random
import time
import logging


def main():
    # Create new client
    client = FastInfluxDBClient.from_config_file("config.toml")
    print(f"{client=}")
    logging.basicConfig(level=logging.DEBUG)
    # logger = logging.getLogger("influxdb_client")
    # logger.addHandler(client.get_logging_handler(bucket="logs"))

    # Generate some random data, and send to influxdb server
    while 1:
        data = random.random()
        metric = InfluxMetric(
            measurement="py_metric1",
            fields={"data1": data, "data2": 1},
            # tags={"host": "localhost"},
        )
        client.write_metric(metric, bucket="metrics")
        logging.info(f"Sent metric: {metric}")
        time.sleep(1)


if __name__ == "__main__":
    main()
