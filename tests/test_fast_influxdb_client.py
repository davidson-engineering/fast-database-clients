import pytest
from datetime import datetime, timedelta
from fast_influxdb_client.fast_influxdb_client import FastInfluxDBClient, InfluxMetric, ClientEnvVariableNotDefined, InfluxDBWriteError

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
    metric = InfluxMetric(measurement=TEST_MEASUREMENT, fields=TEST_FIELDS, time=TEST_TIME)
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
    metric = InfluxMetric(measurement=TEST_MEASUREMENT, fields=TEST_FIELDS, time=TEST_TIME)
    
    with pytest.raises(InfluxDBWriteError):
        client.write_metric(metric)


