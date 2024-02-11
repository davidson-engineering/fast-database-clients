import pytest
from datetime import datetime

from fast_database_clients.fast_influxdb_client import (
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
        measurement="test_measurement", fields={"value": 42}, time=datetime.now(), write_precision=write_precision
    )
    fast_influxdb_client.write(metric)
    
    write_precision = "us"
    fast_influxdb_client.default_write_precision = write_precision
    metric = InfluxMetric(
        measurement="test_measurement", fields={"value": 42}, time=datetime.now(), write_precision=write_precision
    )
    fast_influxdb_client.write(metric)
    
    write_precision = "ns"
    fast_influxdb_client.default_write_precision = write_precision
    metric = InfluxMetric(
        measurement="test_measurement", fields={"value": 42}, time=datetime.now(), write_precision=write_precision
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
            measurement="test_measurement", fields={"value": 42}, time=datetime.now(), write_precision=write_precision
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
            measurement="test_measurement", fields={"value": 42}, time=datetime.now(), write_precision=write_precision
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
