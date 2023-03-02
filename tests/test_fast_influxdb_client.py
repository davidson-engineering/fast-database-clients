import pytest
from datetime import datetime
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from unittest.mock import patch, MagicMock

from fast_influxdb_client.fast_influxdb_client import FastInfluxDBClient, ClientEnvVariableNotDefined


# These tests check that:

#     The __init__ method raises a ClientEnvVariableNotDefined exception if any of the required environment variables are not defined.
#     The write_data method calls the write_api method of the InfluxDBClient with the correct arguments.
#     The write_data method raises an ApiException if the write_api call fails.
#     The write_metric method calls the write_data method with the correct arguments.


class TestFastInfluxDBClient:
    def test_init_raises_exception_if_env_variables_not_defined(self, monkeypatch):
        # Arrange
        monkeypatch.delenv("TOKEN", raising=False)
        monkeypatch.delenv("CLIENT_URL", raising=False)
        monkeypatch.delenv("ORG", raising=False)
        monkeypatch.delenv("BUCKET", raising=False)

        # Act and Assert
        with pytest.raises(ClientEnvVariableNotDefined):
            client = FastInfluxDBClient()

    @patch("fast_influxdb_client.InfluxDBClient")
    def test_write_data_calls_write_api_with_correct_arguments(self, influxdb_client_mock):
        # Arrange
        client = FastInfluxDBClient()
        write_api_mock = MagicMock()
        influxdb_client_mock.return_value.write_api.return_value = write_api_mock
        data = [{"measurement": "measurement_name", "fields": {"field_name": 1}, "time": datetime.utcnow()}]

        # Act
        client.write_data(data, write_option=SYNCHRONOUS)

        # Assert
        write_api_mock.write.assert_called_once_with(bucket=client.bucket, org=client.org, record=data, write_precision="s")

    @patch("fast_influxdb_client.InfluxDBClient")
    def test_write_data_raises_exception_if_write_api_call_fails(self, influxdb_client_mock):
        # Arrange
        client = FastInfluxDBClient()
        write_api_mock = MagicMock()
        write_api_mock.write.side_effect = ApiException(status=400, reason="Bad Request")
        influxdb_client_mock.return_value.write_api.return_value = write_api_mock
        data = [{"measurement": "measurement_name", "fields": {"field_name": 1}, "time": datetime.utcnow()}]

        # Act and Assert
        with pytest.raises(ApiException):
            client.write_data(data, write_option=SYNCHRONOUS)

    @patch("fast_influxdb_client.InfluxDBClient")
    def test_write_metric_calls_write_data_with_correct_arguments(self, influxdb_client_mock):
        # Arrange
        client = FastInfluxDBClient()
        write_data_mock = MagicMock()
        client.write_data = write_data_mock
        fields = {"field_name": 1}

        # Act
        client.write_metric(name="measurement_name", fields=fields, time=datetime.utcnow())

        # Assert
        write_data_mock.assert_called_once_with([{"measurement": "measurement_name", "fields": fields, "time": datetime.utcnow()}], write_option=SYNCHRONOUS)

