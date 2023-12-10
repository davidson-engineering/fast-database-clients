


from fast_influxdb_client import FastInfluxDBClient
from datetime import datetime, timezone



client = FastInfluxDBClient.from_config_file('config.toml')




metric = dict(
    measurement='cpu', fields=dict(usage=3.0), time=datetime.now(timezone.utc)
)
client.write_api().write(bucket='metrics', record=metric)
print(client)