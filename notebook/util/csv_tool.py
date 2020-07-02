import os
import re

import pandas as pd


class MergedDataFrameReader:
    def __init__(self, directory_name, columns):
        self.directory_name = directory_name
        self.columns = columns

    def read_csv(self, filename):
        try:
            return pd.read_csv(filename, names=self.columns, header=None)
        except:
            return pd.DataFrame()

    def read_csvs_as_one(self):
        files = os.listdir(self.directory_name)
        files = filter(lambda file: re.match('.+\\.csv', file), files)
        files = map(lambda file: os.path.join(self.directory_name, file), files)
        dfs = map(self.read_csv, files)
        return pd.concat(dfs)
