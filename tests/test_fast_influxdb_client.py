import pytest
from datetime import datetime
from fast_influxdb_client.fast_influxdb_client import (
    FastInfluxDBClient,
    InfluxMetric,
    ClientEnvVariableNotDefined,
    InfluxDBWriteError,
)
from influxdb_client.client.write_api import SYNCHRONOUS


# Define some constants for testing
TEST_MEASUREMENT = "test_measurement"
TEST_FIELDS = {"field1": 42, "field2": "value"}
TEST_TIME = datetime.utcnow()


# Mocking the InfluxDB server
class MockInfluxDBClient:
    def __init__(self):
        # Add any necessary initialization logic for your mock
        pass

    def write_api(self, write_option):
        return MockWriteApi()

    def close(self):
        # Implement the close method behavior if needed
        pass


class MockWriteApi:
    def write(self, bucket, org, metric, write_precision):
        pass


def test_fast_influxdb_client_init(monkeypatch):
    # Mock the environment variables
    monkeypatch.setenv("TOKEN", "test_token")
    monkeypatch.setenv("CLIENT_URL", "test_url")
    monkeypatch.setenv("ORG", "test_org")
    monkeypatch.setenv("BUCKET", "test_bucket")

    # Test initialization with valid environment variables
    with FastInfluxDBClient() as client:
        assert client.org == "test_org"
        assert client.bucket == "test_bucket"

    # Test initialization with missing environment variables
    with pytest.raises(ClientEnvVariableNotDefined):
        with FastInfluxDBClient(env_filepath="nonexistent_file"):
            pass


def test_fast_influxdb_client_write_metric(monkeypatch):
    # Mock the InfluxDB client
    monkeypatch.setattr(FastInfluxDBClient, "client", MockInfluxDBClient())

    # Test writing a metric
    client = FastInfluxDBClient()
    metric = InfluxMetric(
        measurement=TEST_MEASUREMENT, fields=TEST_FIELDS, time=TEST_TIME
    )
    client.write_metric(metric)


def test_fast_influxdb_client_write_data(monkeypatch):
    # Mock the InfluxDB client
    monkeypatch.setattr(FastInfluxDBClient, "client", MockInfluxDBClient())

    # Test writing data
    client = FastInfluxDBClient()
    client.write_data(measurement=TEST_MEASUREMENT, fields=TEST_FIELDS, time=TEST_TIME)


def test_fast_influxdb_client_write_data_default_time(monkeypatch):
    # Mock the InfluxDB client
    monkeypatch.setattr(FastInfluxDBClient, "client", MockInfluxDBClient())

    # Test writing data with default time (datetime.utcnow())
    client = FastInfluxDBClient()
    client.write_data(measurement=TEST_MEASUREMENT, fields=TEST_FIELDS)


def test_fast_influxdb_client_exit(monkeypatch):
    # Mock the InfluxDB client
    monkeypatch.setattr(FastInfluxDBClient, "client", MockInfluxDBClient())

    # Test __exit__ method
    with FastInfluxDBClient() as client:
        pass  # If __exit__ doesn't raise an exception, the test passes


def test_fast_influxdb_client_write_error(monkeypatch):
    # Mock the InfluxDB client to raise an arbitrary exception
    class MockInfluxDBClientWithException:
        def write_api(self, write_option):
            return MockWriteApiWithException()

    class MockWriteApiWithException:
        def write(self, bucket, org, metric, write_precision):
            raise Exception("test exception")

    monkeypatch.setattr(FastInfluxDBClient, "client", MockInfluxDBClientWithException())

    # Test InfluxDBWriteError exception
    client = FastInfluxDBClient()
    metric = InfluxMetric(
        measurement=TEST_MEASUREMENT, fields=TEST_FIELDS, time=TEST_TIME
    )

    with pytest.raises(InfluxDBWriteError):
        client.write_metric(metric)


class TestDataTypes:
    @pytest.fixture
    def influx_client(self, monkeypatch):
        # Mock the InfluxDB client
        monkeypatch.setattr(FastInfluxDBClient, "client", MockInfluxDBClient())
        return FastInfluxDBClient()

    def test_write_metric_with_string_field(self, influx_client):
        # Test writing a metric with a string field
        metric = InfluxMetric(
            measurement="test_measurement", fields={"field_name": "string_value"}
        )
        influx_client.write_metric(metric, write_option=SYNCHRONOUS)

        # Add assertions if needed

    def test_write_metric_with_integer_field(self, influx_client):
        # Test writing a metric with an integer field
        metric = InfluxMetric(measurement="test_measurement", fields={"field_name": 42})
        influx_client.write_metric(metric, write_option=SYNCHRONOUS)

        # Add assertions if needed

    def test_write_metric_with_float_field(self, influx_client):
        # Test writing a metric with a float field
        metric = InfluxMetric(
            measurement="test_measurement", fields={"field_name": 3.14}
        )
        influx_client.write_metric(metric, write_option=SYNCHRONOUS)

        # Add assertions if needed

    def test_write_metric_with_boolean_field(self, influx_client):
        # Test writing a metric with a boolean field
        metric = InfluxMetric(
            measurement="test_measurement", fields={"field_name": True}
        )
        influx_client.write_metric(metric, write_option=SYNCHRONOUS)

        # Add assertions if needed

    def test_write_metric_with_datetime_field(self, influx_client):
        # Test writing a metric with a datetime field
        now = datetime.utcnow()
        metric = InfluxMetric(
            measurement="test_measurement", fields={"field_name": now}
        )
        influx_client.write_metric(metric, write_option=SYNCHRONOUS)

        # Add assertions if needed

    def test_write_metric_with_multiple_field_types(self, influx_client):
        # Test writing a metric with fields of different types
        metric = InfluxMetric(
            measurement="test_measurement",
            fields={
                "string_field": "string_value",
                "int_field": 42,
                "float_field": 3.14,
                "bool_field": True,
                "datetime_field": datetime.utcnow(),
            },
        )
        influx_client.write_metric(metric, write_option=SYNCHRONOUS)

        # Add assertions if needed

    def test_write_metric_with_invalid_field_type(self, influx_client):
        # Test writing a metric with an invalid field type
        metric = InfluxMetric(
            measurement="test_measurement", fields={"field_name": complex(1, 2)}
        )

        # Ensure InfluxDBWriteError is raised for invalid field type
        with pytest.raises(InfluxDBWriteError):
            influx_client.write_metric(metric, write_option=SYNCHRONOUS)
