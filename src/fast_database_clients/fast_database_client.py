#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from abc import ABC, abstractmethod
from collections import deque
import threading
import time

MAX_BUFFER_LENGTH = 65_536
WRITE_BATCH_SIZE = 5_000


class DatabaseClientBase(ABC):

    def __init__(self, buffer=None, write_interval=0.5, **kwargs):
        self._kwargs = kwargs
        self._client = None
        self._last_write_time = time.time()
        self.write_interval = write_interval
        if buffer is not None:
            self.buffer = buffer
        else:
            self.buffer = deque(maxlen=MAX_BUFFER_LENGTH)

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
        while True:
            time_condition = (time.time() - self._last_write_time) > self.write_interval
            if self.buffer and (len(self.buffer) > WRITE_BATCH_SIZE or time_condition):
                metrics = tuple(
                    self.buffer.popleft()
                    for _ in range(min(WRITE_BATCH_SIZE, len(self.buffer)))
                )
                self.write(metrics)
                self._last_write_time = time.time()

    def start(self):
        self.write_thread = threading.Thread(
            target=self.write_periodically, daemon=True
        )
        self.write_thread.start()

    def __del__(self):
        self.write_thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
