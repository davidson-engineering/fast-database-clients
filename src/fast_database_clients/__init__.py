__version__ = "2.1.0"

from fast_database_clients.fast_database_client import DatabaseClientBase
from fast_database_clients.fast_influxdb_client import (
    FastInfluxDBClient,
    InfluxLog,
    InfluxMetric,
    InfluxLoggingHandler,
)
