"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

from itertools import zip_longest
from colcleaner.metrics.base import BaseMetric

__author__ = 'willmcginnis'


class NumericSpan():
    def __init__(self):
        self.max = None
        self.min = None

    def update(self, value):
        value = float(value)
        if self.max is None or value > self.max:
            self.max = value
        if self.min is None or value < self.min:
            self.min = value

    def __repr__(self):
        return '{Between %s-%s}' % (self.min, self.max, )

class StrSpan():
    def __init__(self):
        self.values = set()

    def update(self, value):
        self.values.update({value})

    def __repr__(self):
        if len(self.values) == 1:
            return list(self.values)[0]
        else:
            return '{%s}' % ('|'.join((str(x) for x in self.values)))


class DateFormat(BaseMetric):
    delimiter_options = ['/', ':', '-', ' ']
    master_delim = '|'

    def __init__(self):
        self.delimiters = dict()
        self.values = dict()

    async def update(self, value):
        value = str(value).strip()
        # update the dictionary of delimiter sets based on index in passed string
        order = 0
        for char in value:
            if char in self.delimiter_options:
                self.delimiters[order] = self.delimiters.get(order, StrSpan())
                self.delimiters[order].update(char)
                order += 1

        for delim in self.delimiter_options:
            value = value.replace(delim, self.master_delim)

        value = value.split(self.master_delim)
        for idx, val in enumerate(value):
            # first check if it's a number
            if val.isnumeric() or val.replace('.', '').isnumeric():
                if idx not in self.values:
                    self.values[idx] = NumericSpan()
                self.values[idx].update(val)
            else:
                if idx not in self.values:
                    self.values[idx] = StrSpan()
                self.values[idx].update(val)

    def report(self):
        schema = zip_longest((v for _, v in self.values.items()), (v for _, v in self.delimiters.items()))
        schema = [y.__repr__() for z in schema for y in z if y is not None]
        return {
            'estimated_schema': ''.join(schema)
        }

    def __repr__(self):
        return '<DateFormat>'