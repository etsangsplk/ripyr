"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

__author__ = 'willmcginnis'

class BaseMetric():

    async def update(self, value: str) -> object:
        return self

    def report(self) -> dict:
        return dict()

    def __repr__(self):
        return '<Metric>'