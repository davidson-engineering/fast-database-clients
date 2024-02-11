from dataclasses import dataclass
from fast_database_clients.fast_influxdb_client import InfluxLog
from datetime import timezone


@dataclass
class Record:
    message: str
    level: str
    name: str
    pathname: str
    lineno: int
    created: int
    msecs: int
    levelno: int
    levelname: str = "INFO"
    funcName: str = "test_func"
    module: str = "test_module"
    processName: str = "test_process"
    process: int = 12345
    threadName: str = "test_thread"
    thread: int = 67890
    asctime: str = "2021-08-04T00:00:00Z"


def test_influx_log_creation():
    # create mock logging record
    record = Record(
        message="test message",
        level="INFO",
        name="test_logger",
        pathname="test_pathname",
        lineno=1,
        created=1628083200,
        msecs=0,
        levelno=20,
    )
    log = InfluxLog(record=record, msg="test message")
    assert log is not None
    assert isinstance(log, InfluxLog)
    assert log.measurement == "logs"
    assert log.fields == {
        "level": 20,
        "msg": "test message",
        "name": "test_logger",
        "path": "test_pathname",
        "lineno": 1,
    }
    assert log.tags == {
        "details": "",
        "level": "INFO",
        "path": "test_pathname",
        "lineno": 1,
        "logger": "test_logger",
        "function": "test_func()",
        "module": "test_module",
        "process": "test_process:12345",
        "thread": "test_thread:67890",
        "record_time": "2021-08-04T00:00:00Z,0",
    }
    assert log.time is not None
    assert log.time.tzinfo is not None
    assert log.time.tzinfo.utcoffset(log.time) == timezone.utc.utcoffset(log.time)
    assert log.time.timestamp() == 1628083200
    assert log[0] == "logs"
