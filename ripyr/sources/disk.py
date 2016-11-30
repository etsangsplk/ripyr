"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

import csv
import json

__author__ = 'willmcginnis'


class CSVDiskSource(object):
    def __init__(self, filename: str, mode: str = 'r', quotechar: str = '"') -> None:
        self.filename = filename
        self.f_handle = open(filename, mode)
        self.quotechar = quotechar
        self.header = [x.strip().replace(self.quotechar, '') for x in self.f_handle.readline().split(',')]

    def __next__(self) -> dict:
        data = csv.reader([self.f_handle.readline()], delimiter=',', quotechar=self.quotechar).__next__()
        if data:
            return dict(zip(self.header, data))
        else:
            self.f_handle.close()
            raise StopIteration

    def __del__(self) -> None:
        self.f_handle.close()


class RowJSONDiskSource(object):
    def __init__(self, filename: str, mode: str = 'r') -> None:
        self.filename = filename
        self.f_handle = open(filename, mode)

    def __next__(self) -> dict:
        data = self.f_handle.readline()
        if data:
            return json.loads(data)
        else:
            self.f_handle.close()
            raise StopIteration

    def __del__(self) -> None:
        self.f_handle.close()
