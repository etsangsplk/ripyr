"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

from colcleaner.metrics.base import BaseMetric

__author__ = 'willmcginnis'


class TypingMetric(BaseMetric):
    def __init__(self) -> None:
        self.type_estimate = None

    async def update(self, value) -> BaseMetric:
        # first figure out the type of whatever was passed in
        if str(value).isnumeric():
            est = 'int'
        elif str(value).isalpha() == True or str(value).isalnum() == True:
            est = 'str'
        elif str(value).lower() == 'true' or str(value).lower() == 'false' or str(value).lower() == 't' or str(value).lower() == 'f':
            est = 'bool'
        else:
            try:
                float(str(value))
                est = 'float'
            except:
                est = 'str'

        # then upsert into current state
        if self.type_estimate is None:
            self.type_estimate = est
            return self

        # if nothing has changed, or we've seen a string, do nothing.
        if self.type_estimate == est or self.type_estimate == 'str':
            return self

        # if we got a string, it's a string forever
        if est == 'str':
            self.type_estimate = 'str'
            return self

        # if we got an int but through it was float, float wins
        if est == 'int' and self.type_estimate == 'float':
            return self

        # if we got a float but throught it was an int, float wins
        if est == 'float' and self.type_estimate == 'int':
            self.type_estimate = 'float'
            return self

        # otherwise just update
        self.type_estimate = est
        return self

    def report(self) -> dict:
        return {
            "type": self.type_estimate
        }

    def __repr__(self):
        return '<Typing>'