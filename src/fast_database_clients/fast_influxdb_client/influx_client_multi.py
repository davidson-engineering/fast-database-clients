from collections import deque
from dataclasses import asdict, dataclass
from multiprocessing import Process, JoinableQueue, cpu_count
from typing import Any, Dict, Mapping, Union, Iterable
from influxdb_client.client.write_api import WriteOptions, WriteType
import reactivex as rx
from reactivex import operators as ops
from influxdb_client.client.write.point import Point
from influxdb_client.client.exceptions import InfluxDBError
from datetime import datetime, timedelta, timezone
import logging
from influxdb_client import InfluxDBClient
from fast_database_clients.fast_database_client import DatabaseClientBase

logger = logging.getLogger(__name__)


class UnpackMixin(Mapping):
    def __iter__(self):
        return iter(asdict(self).keys())

    def __len__(self):
        return len(asdict(self))

    def __getitem__(self, key):
        if key not in asdict(self):
            raise KeyError(f"Key {key} not found in {self.__class__.__name__}")
        return getattr(self, key)


class DatabaseWriter(Process):
    """
    A writer process for uploading signal data to InfluxDB in batches using multiprocessing.
    """

    def __init__(self, queue: JoinableQueue, config: Dict[str, Any]):
        super().__init__()
        self.queue = queue
        self.config = config
        self.client = None

    def initialize_client(self):
        raise NotImplementedError("initialize_client not implemented.")

    def write(self, batch: Iterable[Dict[str, Any]]):
        raise NotImplementedError("write not implemented.")

    def run(self):
        self.initialize_client()
        if self.client is None:
            msg = "Client not properly initialized."
            logger.error(msg, extra={"client": self.client})
            raise ValueError(msg)
        logger.debug(f"{self.name}: Writer started.")
        while True:
            batch = self.queue.get()
            if batch is None:  # Termination signal
                logger.debug(f"{self.name}: Received termination signal.")
                break
            logger.debug(f"{self.name}: Writing batch of {len(batch)} records.")
            self.write(batch)

        self._close()

    def _close(self):
        logger.debug(f"{self.name}: Flushing and closing InfluxDB client.")
        if self.write_api:
            self.write_api.close()
        if self.client:
            self.client.close()


@dataclass
class InfluxDBWriterConfig:
    url: str
    token: str
    org: str
    bucket: str
    write_precision: str = "ms"


class InfluxDBWriter(DatabaseWriter):

    def initialize_client(self):
        self.client = InfluxDBClient(
            url=self.config.url,
            token=self.config.token,
            org=self.config.org,
        )
        self.write_api = self.client.write_api(
            write_options=WriteOptions(write_type=WriteType.batching)
        )

    def write(self, batch: Iterable[Dict[str, Any]]):

        try:
            self.write_api.write(
                bucket=self.config.bucket,
                record=batch,
                write_precision=self.config.write_precision,
            )
            self.queue.task_done()
        except InfluxDBError as e:
            logger.error(f"{self.name}: Failed to write batch: {e}")


@dataclass
class MultiInfluxDBClientConfig(UnpackMixin):
    url: str
    token: str
    org: str
    bucket: str
    buffer: deque
    timeout: int = 10_000
    num_workers: int = 4
    batch_size: int = 5000
    write_precision: str = "ms"


class MultiInfluxDBClient(DatabaseClientBase):
    """

    A multiprocessing-enabled InfluxDB client for high-performance batch writing using reactivex.

    Extends the functionality of FastInfluxDBClient with multiprocessing
    to handle large-scale data ingestion in parallel.
    """

    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        buffer: deque,
        timeout: int = 10_000,
        num_workers: int = 4,
        batch_size: int = 5_000,
        write_precision: str = "ms",
    ):
        """
        Initialize the MultiProcInfluxDBClient.

        :param num_workers: Number of worker processes.
        :param batch_size: Number of records in each batch.
        :param default_write_precision: Default write precision for timestamps.
        :param org: Organization name.
        :param url: InfluxDB URL.
        :param token: InfluxDB token.
        :param default_bucket: Default bucket name.
        :param timeout: Timeout in milliseconds.
        :param queue: A buffer for communication between the main process and
            worker processes.
        :param write_precision: Write precision for timestamps.

        """
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.timeout = timeout
        self.write_precision = write_precision
        self.buffer = buffer
        self.queue = JoinableQueue()
        self.processes = []

        self.num_workers = (
            cpu_count() if num_workers == -1 else min(num_workers, cpu_count())
        )

        super().__init__(
            buffer=self.buffer, write_batch_size=batch_size * self.num_workers
        )

    def start_workers(self):
        """
        Start worker processes.
        """

        writer_config = InfluxDBWriterConfig(
            url=self.url,
            token=self.token,
            org=self.org,
            bucket=self.bucket,
            write_precision=self.write_precision,
        )

        self.processes = [
            InfluxDBWriter(queue=self.queue, config=writer_config)
            for _ in range(self.num_workers)
        ]

        for process in self.processes:
            process.start()

    def stop_workers(self):
        """
        Stop all worker processes by sending a termination signal.
        """
        for _ in self.processes:
            self.queue.put(None)
        for process in self.processes:
            process.join()

    def write(self, metrics: Union[Iterable[Point], Iterable[dict]]):
        """
        Write metrics to the queue using reactivex for batching.

        :param metrics: Iterable of metrics (Points or dicts).
        """
        observable = rx.from_iterable(metrics)

        observable.pipe(
            ops.buffer_with_count(
                self.batch_size
            ),  # Create batches of size `batch_size`
        ).subscribe(
            on_next=lambda batch: self.queue.put(batch),  # Push batches to the queue
            on_completed=lambda: self.stop_workers(),  # Stop workers when done
            on_error=lambda e: logger.error(f"Error in stream: {e}"),
        )

    def ping(self):
        return NotImplementedError("Ping not implemented for MultiInfluxDBClient.")

    def query(self, query: str, **kwargs):
        return NotImplementedError("Query not implemented for MultiInfluxDBClient.")

    def __enter__(self):
        self.start_workers()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_workers()


# Example Usage
if __name__ == "__main__":

    from config_loader import load_configs

    logging.basicConfig(level=logging.INFO)

    def generate_metrics(count: int):
        """
        Generate example metrics as dictionaries.

        :param count: Number of metrics to generate.
        :return: A generator yielding metrics.
        """
        start_time = datetime.now(timezone.utc) - timedelta(hours=0.5)
        for i in range(count):

            yield {
                "measurement": "example_measurement_multi",
                "fields": {"value": i, "random": i % 10},
                "time": start_time + timedelta(milliseconds=i),
                "tags": {"source": "sensor_1"},
            }

    # Example initialization with similar configuration to FastInfluxDBClient
    config_file = "config/influx_config.toml"
    config = load_configs(config_file)
    buffer = deque()
    influx_client_config = MultiInfluxDBClientConfig(
        url=config["influx2"]["url"],
        buffer=buffer,
        token=config["influx2"]["token"],
        org=config["influx2"]["org"],
        timeout=config["influx2"]["timeout"],
        bucket=config["database_client"]["influx"]["default_bucket"],
        batch_size=config["database_client"]["influx"]["write_batch_size"],
        num_workers=4,
    )
    import time

    start_time = time.perf_counter()

    with MultiInfluxDBClient(**influx_client_config) as client:
        # Generate 100,000 example metrics
        metrics = generate_metrics(1_000_00)
        for metric in metrics:
            buffer.append(metric)
        client.start()

        while len(buffer) > 0:
            time.sleep(0.1)

    end_time = time.perf_counter()

    logger.info(
        "Data ingestion completed in {:.2f} seconds.".format(end_time - start_time)
    )
