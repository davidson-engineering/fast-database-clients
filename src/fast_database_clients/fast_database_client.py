#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from abc import ABC, abstractmethod
from collections import deque
import itertools
import threading
import time
import logging

logger = logging.getLogger(__name__)

MAX_BUFFER_LENGTH = 65_536
WRITE_BATCH_SIZE = 5_000


class DatabaseClientBase(ABC):

    def __init__(
        self,
        buffer=None,
        write_interval=0.5,
        write_batch_size=WRITE_BATCH_SIZE,
        **kwargs,
    ):
        self._kwargs = kwargs
        self._client = None
        self._last_write_time = time.time()
        self.write_interval = write_interval
        self.write_batch_size = write_batch_size
        if buffer is not None:
            self.buffer = buffer
        else:
            self.buffer = deque(maxlen=MAX_BUFFER_LENGTH)
        self._stop_event = threading.Event()

    @abstractmethod
    def ping(self): ...

    @abstractmethod
    def write(self, data, **kwargs): ...

    @abstractmethod
    def query(self, query, **kwargs): ...

    @abstractmethod
    def close(self): ...

    def convert(self, data):
        return data

    def close(self):
        """Shutdown the client."""
        self.__del__()

    def write_periodically(self):
        try:
            self.__enter__()  # Open resources
            while not self._stop_event.is_set():
                time_condition = (
                    time.time() - self._last_write_time
                ) > self.write_interval
                if self.buffer and (
                    len(self.buffer) >= self.write_batch_size or time_condition
                ):
                    now = time.time()
                    batch_size = min(self.write_batch_size, len(self.buffer))
                    metrics = tuple(itertools.islice(self.buffer, batch_size))
                    for _ in range(batch_size):
                        self.buffer.popleft()
                    try:
                        self.write(metrics)
                    except Exception as e:
                        logger.error(f"Write operation failed: {e}", exc_info=True)
                    self._last_write_time = now
                else:
                    time.sleep(min(self.write_interval, 0.1))
        finally:
            self.__exit__(None, None, None)  # Ensure cleanup

    def start(self):
        self.write_thread = threading.Thread(
            target=self.write_periodically, daemon=True
        )
        self.write_thread.start()

    def stop(self):
        self._stop_event.set()

    def __del__(self):
        self.stop()
        self.write_thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
