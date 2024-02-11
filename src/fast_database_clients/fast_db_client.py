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
        import tomli

        with open(filepath, "rb") as file:
            return tomli.load(file)

    # else load as binary
    with open(filepath, "rb") as file:
        return file.read()


class DatabaseClientBase(ABC):
    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self._client = None

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

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
