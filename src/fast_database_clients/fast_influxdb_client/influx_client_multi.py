from multiprocessing import Process, Manager, Queue
from typing import Any, Dict, Union, Iterable
from influxdb_client.client.write_api import WriteOptions, WriteType
import reactivex as rx
from reactivex import operators as ops
from influxdb_client.client.write.point import Point
from influxdb_client.client.exceptions import InfluxDBError
from datetime import datetime, timedelta, timezone
import logging
from influxdb_client import InfluxDBClient
from fast_database_clients.fast_database_client import DatabaseClientBase, load_config

logger = logging.getLogger(__name__)


class InfluxDBWriter(Process):
    """
    A writer process for uploading signal data to InfluxDB in batches using multiprocessing.
    """

    def __init__(self, queue: Queue, influx_config: Dict[str, Any]):
        super().__init__()
        self.queue = queue
        self.influx_config = influx_config
        self.client = None
        self.write_api = None

    def run(self):
        self.client = InfluxDBClient(
            url=self.influx_config["url"],
            token=self.influx_config["token"],
            org=self.influx_config["org"],
        )
        self.write_api = self.client.write_api(
            write_options=WriteOptions(write_type=WriteType.batching)
        )

        logger.debug(f"{self.name}: Writer started.")
        while True:
            batch = self.queue.get()
            if batch is None:  # Termination signal
                logger.debug(f"{self.name}: Received termination signal.")
                break
            try:
                self.write_api.write(bucket=self.influx_config["bucket"], record=batch)
                self.queue.task_done()
            except InfluxDBError as e:
                logger.error(f"{self.name}: Failed to write batch: {e}")

        self._close()

    def _close(self):
        logger.debug(f"{self.name}: Flushing and closing InfluxDB client.")
        if self.write_api:
            self.write_api.close()
        if self.client:
            self.client.close()


class MultiInfluxDBClient(DatabaseClientBase):
    """

    A multiprocessing-enabled InfluxDB client for high-performance batch writing using reactivex.

    Extends the functionality of FastInfluxDBClient with multiprocessing
    to handle large-scale data ingestion in parallel.
    """

    def __init__(
        self,
        influx_config: Dict[str, Any],
        timeout: int = 10_000,
        num_workers: int = 6,
        batch_size: int = 5_000,
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

        """
        self.influx_config = influx_config
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.timeout = timeout
        self.queue = Manager().Queue()
        self.processes = []

    def start_workers(self):
        """
        Start worker processes.
        """
        self.processes = [
            InfluxDBWriter(queue=self.queue, influx_config=self.influx_config)
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

    def generate_metrics(count: int):
        """
        Generate example metrics as dictionaries.

        :param count: Number of metrics to generate.
        :return: A generator yielding metrics.
        """
        start_time = datetime.now(timezone.utc) - timedelta(hours=1)
        for i in range(count):

            yield {
                "measurement": "example_measurement_multi",
                "fields": {"value": i, "random": i % 10},
                "time": start_time + timedelta(milliseconds=i),
                "tags": {"source": "sensor_1"},
            }

    # Example initialization with similar configuration to FastInfluxDBClient
    config_file = "config/.influx_live.toml"
    config = load_config(config_file)
    url = config["influx2"]["url"]
    token = config["influx2"]["token"]
    org = config["influx2"]["org"]
    timeout = config["influx2"]["timeout"]
    default_bucket = config["database_client"]["influx"]["default_bucket"]
    default_write_precision = config["database_client"]["influx"][
        "default_write_precision"
    ]
    batch_size = config["database_client"]["influx"]["write_batch_size"]
    influx_config = {
        "url": url,
        "token": token,
        "org": org,
        "bucket": default_bucket,
    }
    import time

    start_time = time.perf_counter()

    with MultiInfluxDBClient(
        influx_config=influx_config,
        batch_size=batch_size,
        timeout=timeout,
        num_workers=6,
    ) as client:
        # Generate 100,000 example metrics
        metrics = generate_metrics(500_000)
        client.write(metrics)

    end_time = time.perf_counter()

    print("Data ingestion completed in {:.2f} seconds.".format(end_time - start_time))
