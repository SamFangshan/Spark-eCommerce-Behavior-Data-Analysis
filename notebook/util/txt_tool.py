import numpy as np


class HistogramDataPointReader:
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        intervals = list()
        counts = list()
        with open(self.filename, 'r') as f:
            lines = f.readlines()
            intervals = np.array(list(map(lambda n: float(n), lines[0].replace("\x00", '').split(','))))
            counts = np.array(list(map(lambda n: int(n), lines[1].replace("\x00", '').split(','))))
        return intervals, counts
