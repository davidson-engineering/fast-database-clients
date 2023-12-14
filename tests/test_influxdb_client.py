import pytest
from datetime import datetime, timezone
from fast_influxdb_client import (
    FastInfluxDBClient,
    InfluxMetric,
    convert_to_seconds,
)
import logging
from unittest.mock import MagicMock


@pytest.fixture
def mocker_influx_client():
    # Create a mock InfluxDBClient
    mock_client = MagicMock(spec=FastInfluxDBClient)

    # Set default_org attribute on the mock client
    mock_client.default_org = "your_default_org"  # Replace with your actual default org
    mock_client.default_bucket = (
        "your_default_bucket"  # Replace with your actual default bucket
    )

    return mock_client


def test_convert_to_seconds():
    assert convert_to_seconds("30d") == 2592000
    assert convert_to_seconds("30m") == 1800
    assert convert_to_seconds("50s") == 50
    assert convert_to_seconds("1d 30m") == 88200


def test_influx_metric_creation():
    measurement = "test_measurement"
    fields = {"field1": 123, "field2": "value"}
    time = datetime.now(timezone.utc)
    tags = {"tag1": "tag_value"}

    metric = InfluxMetric(measurement, fields=fields, time=time, tags=tags)

    assert metric.measurement == measurement
    assert metric.fields == fields
    assert metric.time == time
    assert metric.tags == tags


def test_fast_influxdb_client_creation(mocker_influx_client):
    assert mocker_influx_client is not None
    assert isinstance(mocker_influx_client, FastInfluxDBClient)


def test_fast_influxdb_client_write_metric(mocker_influx_client):
    metric = InfluxMetric(measurement="test_measurement", fields={"value": 42})
    mocker_influx_client.write_metric(metric)


def test_fast_influxdb_client_write_data(mocker_influx_client):
    mocker_influx_client.write_data(
        measurement="test_measurement", fields={"value": 42}
    )


# def test_fast_influxdb_client_create_bucket(mocker_influx_client):
#     bucket_name = "test_bucket"
#     mocker_influx_client.create_bucket(bucket_name)
#     buckets = mocker_influx_client.list_buckets()
#     assert any(bucket.name == bucket_name for bucket in buckets)


def test_fast_influxdb_client_update_bucket(mocker_influx_client):
    bucket_name = "test_bucket"
    mocker_influx_client.create_bucket(bucket_name)
    mocker_influx_client.update_bucket(bucket_name, retention_duration="7d")
