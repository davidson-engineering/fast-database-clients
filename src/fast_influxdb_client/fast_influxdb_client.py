#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-03-01
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------
from datetime import datetime
from dotenv import load_dotenv
import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

class ClientEnvVariableNotDefined(Exception):
    pass

class InfluxDBWriteError(Exception):
    pass

class FastInfluxDBClient:
    """
    A class for sending data to an InfluxDB server using the InfluxDB client API.
    """

    TOKEN_VAR = 'TOKEN'
    CLIENT_URL_VAR = 'CLIENT_URL'
    ORG_VAR = 'ORG'
    BUCKET_VAR = 'BUCKET'

    def __init__(self, envfile=".env"):
        
        if envfile:
            load_dotenv(envfile)
        else:
            load_dotenv()

        token = os.getenv(self.TOKEN_VAR)
        url = os.getenv(self.CLIENT_URL_VAR)
        org = os.getenv(self.ORG_VAR)
        bucket = os.getenv(self.BUCKET_VAR)

        for var_name in [self.TOKEN_VAR, self.CLIENT_URL_VAR, self.ORG_VAR, self.BUCKET_VAR]:
            if os.getenv(var_name) is None:
                raise ClientEnvVariableNotDefined(f"{var_name} is not defined in .env file")

        self.__class__.client = InfluxDBClient(url=url, token=token)
        self.org = org
        self.bucket = bucket

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def write_data(self, data, write_option=SYNCHRONOUS):
        try:
            write_api = self.__class__.client.write_api(write_option)
            write_api.write(self.bucket, self.org , data, write_precision='s')
        except Exception as e:
            logging.error(f"Failed to write data to InfluxDB: {e}")
            raise InfluxDBWriteError from e

    def write_metric(self, name:str, fields:dict, time = datetime.utcnow()):
        influx_metric = [{
            'measurement': name,
            'time': time,
            'fields': fields
        }]
        #Saving data to InfluxDB
        self.write_data(influx_metric)