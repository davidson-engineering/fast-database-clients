# Fast InfluxDB Client

A class to facilitate rapid deployment of a class to send metrics to an InfluxDB server.


```
from fast_influxdb_client.fast_influxdb_client import FastInfluxDBClient, InfluxMetric
import random

def main():
    # Create new client
    client = FastInfluxDBClient()

    # Generate some random data, and send to influxdb server
    data = random.random()
    metric = InfluxMetric(
        measurement='py_metric1',
        fields={'data1':data, 'data2':1}
    )
    client.write_metric(metric)
```
