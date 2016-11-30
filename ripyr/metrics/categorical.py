"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

import mmh3
import math
from bitarray import bitarray
from ripyr.metrics.base import BaseMetric

__author__ = 'willmcginnis'

class CardinalityMetric(BaseMetric):
    def __init__(self, size: int=1000, hash_count: int=5) -> None:
        """
        Uses a bloom filter to track an approximate cardinality for the column.

        :param size:
        :param hash_count:
        :return:
        """

        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)

    async def update(self, value: str) -> object:
        value = str(value)
        for seed in range(self.hash_count):
            result = mmh3.hash(value, seed) % self.size
            if self.bit_array[result] == 0:
                for seed in range(self.hash_count):
                    result = mmh3.hash(value, seed) % self.size
                    self.bit_array[result] = 1

        return self

    def approxiate_size(self):
        proportion_empty = 1 - (sum(self.bit_array) / self.size)
        if proportion_empty == 0.0:
            return None
        else:
            return -1 * (self.size / self.hash_count) * math.log(proportion_empty)

    def report(self) -> dict:
        return {
            "approximate_cardinality": self.approxiate_size()
        }

    def __repr__(self):
        return '<Cardinality>'
