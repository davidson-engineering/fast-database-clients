#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-11-19
# version ='1.0'
# ---------------------------------------------------------------------------
"""Demonstration of how to use the FastInfluxDBClient class to send metrics to InfluxDB server"""
# ---------------------------------------------------------------------------

from fast_influxdb_client.fast_influxdb_client import FastInfluxDBClient, InfluxMetric
import random
import time

def main():
    # Create new client
    client = FastInfluxDBClient()
    print(f"{client=}")

    # Generate some random data, and send to influxdb server
    while 1:
        data = random.random()
        metric = InfluxMetric(
            measurement='py_metric1',
            fields={'data1':data, 'data2':1}
        )
        client.write_metric(metric)
        print(f"{data=}")
        time.sleep(5)


if __name__ == "__main__":
    main()
