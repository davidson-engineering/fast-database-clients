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

Rich logging to influxdb is supported via the InfluxLoggingHandler class.

```python
import logging
from fast_influxdb_client import FastInfluxDBClient

config_file = "config.toml"
client = FastInfluxDBClient.from_config_file(config_file=config_file)
client.create_bucket("logs")
client.default_bucket = "logs"

influx_handler = client.get_logging_handler()

logger = logging.getLogger("fast_influxdb_client.fast_influxdb_client")

logger.setLevel(logging.DEBUG)
influx_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    fmt="%(name)s - %(levelname)s - %(message)s",
    datefmt="%Y%m%d %H:%M:%S",
)
influx_handler.setFormatter(formatter)

logger.addHandler(influx_handler)

logger.info("This log gets sent to InfluxDB")
```
Output as viewed in Grafana, showing log details
![image](https://github.com/davidson-engineering/fast-influxdb-client/assets/106140501/d1905d13-4be1-4f6e-bf4a-d583fb563d82)
