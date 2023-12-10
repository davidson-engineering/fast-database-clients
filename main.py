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
from datetime import datetime, timezone

def main():
    bucket="metrics2"
    config_file = "config.toml"
    # Create new client
    client = FastInfluxDBClient.from_config_file(config_file=config_file)
    print(f"{client=}")
    client.default_bucket = bucket
    # client.create_bucket(bucket)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(client.get_logging_handler())

    # Generate some random data, and send to influxdb server
    while 1:
        data = random.random()
        metric = InfluxMetric(
            measurement="py_metric1",
            fields={"data1": data, "data2": 1},
            time=datetime.now(timezone.utc),
        )
        client.write_metric(metric)
        logging.info(f"Sent metric: {metric}")
        time.sleep(1)


if __name__ == "__main__":
    main()
