#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path
from collections import deque
import threading
import time
import asyncio

MAX_BUFFER_LENGTH = 65_536
WRITE_BATCH_SIZE = 5_000


def load_config(filepath: Union[str, Path]) -> dict:
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # if extension is .json
    if filepath.suffix == ".json":
        import json

        with open(filepath, "r") as file:
            return json.load(file)

    # if extension is .yaml
    if filepath.suffix == ".yaml":
        import yaml

        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    # if extension is .toml
    if filepath.suffix == ".toml":
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        with open(filepath, "rb") as file:
            return tomllib.load(file)

    # else load as binary
    with open(filepath, "rb") as file:
        return file.read()


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

    # async def async_drain(self):
    #     """Flush the buffer to the database asynchronously."""
    #     if self._buffer:
    #         await self.write(self._buffer)
    #         self._buffer = []

    # def drain(self):
    #     """Start a new thread to flush the buffer to the database."""
    #     import threading

    #     threading.Thread(target=self._drain_thread).start()

    # def _drain_thread(self):
    #     """Wrapper method for threaded draining."""
    #     asyncio.run(self.async_drain())

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
