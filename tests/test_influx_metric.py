from datetime import timezone


def test_influx_metric():
    from fast_db_clients.fast_influxdb_client import InfluxMetric
    from datetime import datetime

    metric = InfluxMetric(
        measurement="test_metric", time=datetime.now(), fields={"value": 0.5}
    )
    assert metric.measurement == "test_metric"
    assert metric.time is not None
    assert metric.fields == {"value": 0.5}
    assert metric.tags == {}

    metric = InfluxMetric(
        measurement="test_metric",
        time=datetime.now(),
        fields={"value": 0.5},
        tags={"tag1": "value1", "tag2": "value2"},
    )
    assert metric.measurement == "test_metric"
    assert metric.time is not None
    assert metric.fields == {"value": 0.5}
    assert metric.tags == {"tag1": "value1", "tag2": "value2"}
    assert len(metric) == 4
    assert list(metric) == [
        "test_metric",
        metric.time,
        {"value": 0.5},
        {"tag1": "value1", "tag2": "value2"},
    ]

    measurement = "test_measurement"
    fields = {"field1": 123, "field2": "value"}
    time = datetime.now(timezone.utc)
    tags = {"tag1": "tag_value"}

    metric = InfluxMetric(measurement, fields=fields, time=time, tags=tags)

    assert metric.measurement == measurement
    assert metric.fields == fields
    assert metric.time == time.timestamp()
    assert metric.tags == tags
