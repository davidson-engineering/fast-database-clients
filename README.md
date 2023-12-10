# Fast InfluxDB Client

A class to facilitate rapid deployment of a class to send metrics to an InfluxDB server.


```python
from fast_influxdb_client import FastInfluxDBClient, InfluxMetric
import random
import time
from datetime import datetime, timezone

bucket = "metrics"
config_file = "config.toml"
# Create new client
client = FastInfluxDBClient.from_config_file(config_file=config_file)

# Generate some random data, and send to influxdb server
while 1:
    data = random.random()
    data2 = random.randint(0, 100)
    data3 = random.choice([True, False])

    metric = InfluxMetric(
        measurement="py_metric1",
        fields={"data1": data, "data2": data2, "data3": data3},
        time=datetime.now(timezone.utc),
    )

    client.write_metric(metric)
    time.sleep(10)

```
