import pytest
from fast_database_clients import FastInfluxDBClient


@pytest.fixture
def fast_influxdb_client():
    config_file = "tests/influxdb_testing_config.toml"
    # config_file = "config.toml"
    with FastInfluxDBClient.from_config_file(config_file=config_file) as client:
        yield client
        client.close()
