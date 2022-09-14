from influxdb_client import InfluxDBClient
import constants as cons
import pandas as pd


class InfluxDataMini:
    """
    Class for querying InfluxDB or reading already-downloaded
    data from InfluxDB.

    If reading from file, data is expected in
    `csv` format.

    If reading from InfluxDB instance, your configuration
    file `config.ini` must be present in the working directory.
    """
    def __init__(self, file_name=None):
        self.space_keys = ['active_core_count', 'bytes_recv', 'bytes_sent', 'concurrency', 'dropin', 'dropout', 'jobId',
                           'rtt', 'latency', 'parallelism', 'pipelining', 'jobSize', 'packets_sent', 'packets_recv',
                           'errin', 'errout', 'totalBytesSent', 'memory', 'throughput', 'avgJobSize', 'freeMemory']

        self.client = InfluxDBClient.from_config_file("config.ini")
        self.query_api = self.client.query_api()

        # Replace _APP_NAME with your ODS transfer service node-id
        self.p = {
            '_APP_NAME': cons.APP_NAME,
            '_TIME': cons.QUERY_RANGE,
        }

        self.input_file = file_name

    def query_space(self):
        """
        Query's the InfluxDB instance for `QUERY_RANGE` time of data.

        :return: Pandas dataframe
        """

        q = '''from(bucket: "{username}")
  |> range(start: -1h)
  |> fill(usePrevious: true)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
  |> filter(fn: (r) => r["_measurement"] == "transfer_data")
  |> filter(fn: (r) => r["APP_NAME"] == _APP_NAME)'''.format(username=cons.USER_NAME)

        data_frame = self.query_api.query_data_frame(q, params=self.p)
        return data_frame

    def prune_df(self, df):
        """
        Prunes queried data to required fields.

        :param df: Pandas dataframe of queried data
        :return: pruned Pandas dataframe
        """
        df2 = df[self.space_keys]
        # print(df2.tail())
        return df2

    def read_file(self):
        """
        Reads CSV Influx file in working directory
        :return: Pandas dataframe
        """
        data_frame = None
        if self.input_file is not None:
            data_frame = pd.read_csv(self.input_file)
        return data_frame

    def close_client(self):
        """
        Closes Influx Client connection
        """
        self.client.close()
