import numpy as np
from pandas import isna

from influxdb_client import InfluxDBClient
from math import floor


class InfluxData:
    def __init__(self):
        self.space_keys = ['active_core_count', 'bytes_recv', 'bytes_sent', 'concurrency', 'dropin', 'dropout', 'jobId',
                           'rtt', 'latency', 'parallelism', 'pipelining', 'jobSize', 'packets_sent', 'packets_recv',
                           'errin', 'errout', 'totalBytesSent', 'memory', 'throughput', 'avgJobSize', 'freeMemory']

        self.client = InfluxDBClient.from_config_file("config.ini")
        self.query_api = self.client.query_api()

        self.p = {
            '_APP_NAME': "elvisdav@buffalo.edu-didclab-elvis-uc",
            '_TIME': '-2m',
        }

    def query_space(self):
        q = '''from(bucket: "elvisdav@buffalo.edu")
  |> range(start: -5m)
  |> fill(usePrevious: true)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
  |> filter(fn: (r) => r["_measurement"] == "transfer_data")
  |> filter(fn: (r) => r["APP_NAME"] == _APP_NAME)'''

        data_frame = self.query_api.query_data_frame(q, params=self.p)
        # print(data_frame.tail())
        # print(data_frame.columns)
        # print(data_frame)
        return data_frame

    def prune_df(self, df):
        df2 = df[self.space_keys]
        # print(df2.tail())
        return df2

    def close_client(self):
        self.client.close()
