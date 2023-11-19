#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_project_description"""
# ---------------------------------------------------------------------------

from fast_influxdb_client.fast_influxdb_client import FastInfluxDBClient, InfluxMetric
import random
import time

def main():
    client = FastInfluxDBClient()
    print(f"{client=}")

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
