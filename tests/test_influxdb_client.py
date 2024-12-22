import pytest
from datetime import datetime
from unittest.mock import patch
from datetime import datetime
from fast_database_clients.fast_influxdb_client.influx_client import (
    FastInfluxDBClient,
    convert_to_seconds,
    dict_to_point,
    InfluxMetric,
    verify_write_precision,
    localize_time,
)


from config_loader import load_configs


def test_create_client_from_file(config_file):

    config = load_configs(config_file)

    client = FastInfluxDBClient.from_config_file(config_file)
    assert client is not None
    assert isinstance(client, FastInfluxDBClient)
    assert (
        client.default_bucket == config["database_client"]["influx"]["default_bucket"]
    )
    assert (
        client.default_write_precision
        == config["database_client"]["influx"]["default_write_precision"]
    )
    assert client._client.url == config["influx2"]["url"]
    assert client._client.token == config["influx2"]["token"]
    assert client._client.org == config["influx2"]["org"]
    assert (
        client._client.api_client.configuration.timeout == config["influx2"]["timeout"]
    )


def test_convert_to_seconds():
    assert convert_to_seconds("30d") == 2592000
    assert convert_to_seconds("30m") == 1800
    assert convert_to_seconds("50s") == 50
    assert convert_to_seconds("1d 30m") == 88200


def test_fast_influxdb_client_creation(fast_influxdb_client):
    assert fast_influxdb_client is not None
    assert isinstance(fast_influxdb_client, FastInfluxDBClient)


def test_fast_influxdb_client_write_metric(fast_influxdb_client):
    metric = InfluxMetric(
        measurement="test_measurement", fields={"value": 42}, time=datetime.now()
    )
    fast_influxdb_client.write(metric)


def test_fast_influxdb_client_write_data(fast_influxdb_client):
    fast_influxdb_client.write(
        dict(measurement="test_measurement", fields={"value": 42}, time=datetime.now())
    )


def test_fast_influxdb_client_time_precision(fast_influxdb_client):

    import time

    time.sleep(1)

    write_precision = "ms"
    fast_influxdb_client.default_write_precision = write_precision
    metric = InfluxMetric(
        measurement="test_measurement",
        fields={"value": 42},
        time=datetime.now(),
        write_precision=write_precision,
    )
    fast_influxdb_client.write(metric)

    write_precision = "us"
    fast_influxdb_client.default_write_precision = write_precision
    metric = InfluxMetric(
        measurement="test_measurement",
        fields={"value": 42},
        time=datetime.now(),
        write_precision=write_precision,
    )
    fast_influxdb_client.write(metric)

    write_precision = "ns"
    fast_influxdb_client.default_write_precision = write_precision
    metric = InfluxMetric(
        measurement="test_measurement",
        fields={"value": 42},
        time=datetime.now(),
        write_precision=write_precision,
    )
    fast_influxdb_client.write(metric)
    query = f'from(bucket:"{fast_influxdb_client.default_bucket}") |> range(start: -1s)'
    result = fast_influxdb_client.query(query)
    assert len(result[0].records) == 3

    time.sleep(1)

    # Attempt to write 3 metrics with low time precision.
    # Only the last metric should be written, as the time precision is too low to distinguish between the metrics.
    write_precision = "s"
    fast_influxdb_client.default_write_precision = write_precision
    for _ in range(3):
        metric = InfluxMetric(
            measurement="test_measurement",
            fields={"value": 42},
            time=datetime.now(),
            write_precision=write_precision,
        )
        fast_influxdb_client.write(metric)
    query = f'from(bucket:"{fast_influxdb_client.default_bucket}") |> range(start: -1s)'
    result = fast_influxdb_client.query(query)
    assert len(result[0].records) == 1

    time.sleep(2)

    # Attempt to write 5 metrics with high time precision.
    # All the metrics should be written, as the time precision is high enough to distinguish between the metrics.

    write_precision = "ms"
    fast_influxdb_client.default_write_precision = write_precision
    for _ in range(5):
        metric = InfluxMetric(
            measurement="test_measurement",
            fields={"value": 42},
            time=datetime.now(),
            write_precision=write_precision,
        )
        fast_influxdb_client.write(metric)
    query = f'from(bucket:"{fast_influxdb_client.default_bucket}") |> range(start: -2s)'
    result = fast_influxdb_client.query(query)
    assert len(result[0].records) == 5


def test_fast_influxdb_client_create_bucket(fast_influxdb_client):
    bucket_name = "test_bucket"
    fast_influxdb_client.create_bucket(bucket_name)
    buckets = fast_influxdb_client.list_buckets().buckets
    assert any(bucket.name == bucket_name for bucket in buckets)


def test_fast_influxdb_client_update_bucket(fast_influxdb_client):
    bucket_name = "test_bucket"
    fast_influxdb_client.create_bucket(bucket_name)
    fast_influxdb_client.update_bucket(bucket_name, retention_duration="7d")


def test_influxdb_client_ping(fast_influxdb_client):
    assert fast_influxdb_client.ping()


def test_influxdb_version(fast_influxdb_client):
    version = fast_influxdb_client.version()
    assert version is not None
    assert isinstance(version, str)
    assert version.startswith("v2.") or version.startswith("v1.")


def test_verify_write_precision():

    valid_precisions = ["ns", "us", "ms", "s"]
    for precision in valid_precisions:
        assert verify_write_precision(precision) == True

    with pytest.raises(AssertionError):
        verify_write_precision("invalid_precision")


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
    localized = localize_time(dt, "UTC")
    assert localized.tzinfo is not None
    assert localized.tzinfo.zone == "UTC"

    with pytest.raises(ValueError):
        localize_time("invalid_time", "UTC")


def test_dict_to_point():
    data = {
        "measurement": "test_measurement",
        "time": datetime.now(),
        "fields": {"value": 42},
    }
    point = dict_to_point(data)
    assert point is not None
    assert point._name == "test_measurement"


# def test_write():
#     client = FastInfluxDBClient.from_params(
#         url="http://localhost:8086",
#         token="test-token",
#         org="test-org",
#         default_bucket="test-bucket",
#     )
#     with patch.object(
#         client._client.write_api(), "write", return_value=None
#     ) as mock_write:
#         metric = InfluxMetric(
#             measurement="test_measurement", fields={"value": 42}, time=datetime.now()
#         )
#         client.write(metric)
#         mock_write.assert_called_once()


def test_ping():
    client = FastInfluxDBClient.from_params(
        url="http://localhost:8086", token="test-token", org="test-org"
    )
    with patch.object(client._client, "ping", return_value=True) as mock_ping:
        assert client.ping() is True
        mock_ping.assert_called_once()
