"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

from typing import Union
from colcleaner.metrics.base import BaseMetric

__author__ = 'willmcginnis'


class CountMetric(BaseMetric):
    def __init__(self) -> None:
        self.count = 0

    async def update(self, value) -> object:
        self.count += 1
        return self

    def report(self):
        return {
            "count": self.count
        }

    def __repr__(self):
        return '<Count>'


class MinMetric(BaseMetric):
    def __init__(self):
        self.min = None

    async def update(self, value: Union[float, int]) -> object:
        value = float(value)
        try:
            if self.min is None or value < self.min:
                self.min = value
        except TypeError as e:
            pass

        return self

    def report(self) -> dict:
        return {
            "min": self.min
        }

    def __repr__(self):
        return '<Min>'


class MaxMetric(BaseMetric):
    def __init__(self) -> None:
        self.max = None

    async def update(self, value: Union[int, float]) -> object:
        value = float(value)
        try:
            if self.max is None or value > self.max:
                self.max = value
        except TypeError as e:
            pass

        return self

    def report(self) -> dict:
        return {
            "max": self.max
        }

    def __repr__(self):
        return '<Max>'


class Histogram(BaseMetric):
    def __init__(self):
        self.histogram = []

    async def update(self, value: Union[int, float]) -> object:
        value = float(value)


        return self

    def report(self) -> dict:
        return {
            "histogram": []
        }

    def __repr__(self):
        return '<Histogram>'