import pytest

from fast_db_clients.fast_influxdb_client import (
    FastInfluxDBClient,
    InfluxMetric,
    convert_to_seconds,
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
    metric = InfluxMetric(measurement="test_measurement", fields={"value": 42})
    fast_influxdb_client.write(metric)


def test_fast_influxdb_client_write_data(fast_influxdb_client):
    fast_influxdb_client.write(measurement="test_measurement", fields={"value": 42})


def test_fast_influxdb_client_create_bucket(fast_influxdb_client):
    bucket_name = "test_bucket"
    fast_influxdb_client.create_bucket(bucket_name)
    buckets = fast_influxdb_client.list_buckets()
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
    assert version.startswith("2.") or version.startswith("1.")
