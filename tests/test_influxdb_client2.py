import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from fast_database_clients.fast_influxdb_client.influx_client import (
    FastInfluxDBClient,
    FastInfluxDBClientConfigError,
    convert_to_seconds,
    dict_to_point,
    InfluxMetric,
)


def test_verify_write_precision():
    valid_precisions = ["ns", "us", "ms", "s"]
    for precision in valid_precisions:
        assert FastInfluxDBClient.verify_write_precision(precision) == True

    with pytest.raises(ValueError):
        FastInfluxDBClient.verify_write_precision("invalid_precision")


def test_convert_to_seconds():
    assert convert_to_seconds("1d") == 86400
    assert convert_to_seconds("1h") == 3600
    assert convert_to_seconds("1m") == 60
    assert convert_to_seconds("1s") == 1
    assert convert_to_seconds("1d 1h 1m 1s") == 90061

    with pytest.raises(ValueError):
        convert_to_seconds("invalid_format")


def test_localize_time():
    dt = datetime(2024, 1, 1, 12, 0, 0)
    localized = FastInfluxDBClient.localize_time(dt, "UTC")
    assert localized.tzinfo is not None
    assert localized.tzinfo.zone == "UTC"

    with pytest.raises(ValueError):
        FastInfluxDBClient.localize_time("invalid_time", "UTC")


def test_dict_to_point():
    data = {
        "measurement": "test_measurement",
        "time": datetime.now(),
        "fields": {"value": 42},
    }
    point = dict_to_point(data)
    assert point is not None
    assert point.name == "test_measurement"


def test_from_config_file():
    with patch("fast_influxdb_client_refactor.load_config") as mock_load_config:
        mock_load_config.return_value = {
            "database_client": {
                "url": "http://localhost:8086",
                "token": "test-token",
                "default_bucket": "test-bucket",
            }
        }
        client = FastInfluxDBClient.from_config_file("config.toml")
        assert client is not None
        assert client.default_bucket == "test-bucket"

    with pytest.raises(FastInfluxDBClientConfigError):
        FastInfluxDBClient.from_config_file("invalid_config.toml")


def test_write():
    client = FastInfluxDBClient.from_params(
        url="http://localhost:8086", token="test-token", org="test-org"
    )
    with patch.object(
        client._client.write_api(), "write", return_value=None
    ) as mock_write:
        metric = InfluxMetric(
            measurement="test_measurement", fields={"value": 42}, time=datetime.now()
        )
        client.write(metric)
        mock_write.assert_called_once()


def test_ping():
    client = FastInfluxDBClient.from_params(
        url="http://localhost:8086", token="test-token", org="test-org"
    )
    with patch.object(client._client, "ping", return_value=True) as mock_ping:
        assert client.ping() is True
        mock_ping.assert_called_once()
