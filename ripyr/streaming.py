"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

import asyncio
import copy
from typing import List, Union

__author__ = 'willmcginnis'


class StreamingColCleaner(object):
    def __init__(self, source: object = None) -> None:
        self.metrics = {}
        self.global_metrics = []
        self.source = source
        self._event_loop = asyncio.get_event_loop()
        self._cols = set()

    @property
    def columns(self):
        return list(self._cols)

    def add_metric(self, col: str, metric: Union[list, object]) -> object:
        if not isinstance(metric, list):
            metric = [metric]

        if col not in self.metrics:
            self.metrics[col] = metric + [copy.deepcopy(x) for x in self.global_metrics]
        else:
            self.metrics[col] += metric

        return self

    def add_metric_to_all(self, metric: Union[list, object]) -> object:
        if not isinstance(metric, list):
            metric = [metric]

        self.global_metrics += metric

        return self

    def update(self, document: dict) -> object:
        self._cols.update(set(document.keys()))

        for k, v in document.items():
            # if a new column is discovered, then make sure we run global metrics on it
            if k not in self.metrics:
                self.metrics[k] = [copy.deepcopy(x) for x in self.global_metrics]

            for metric in self.metrics.get(k, []):
                self._event_loop.run_until_complete(metric.update(v))

        return self

    def report(self) -> dict:
        self._event_loop.close()
        out = {
            "columns": self.columns,
            "metrics": {
                k: dict(pair for d in v for pair in d.report().items()) for k, v in self.metrics.items()
            }
        }
        del self._event_loop
        self._event_loop = asyncio.get_event_loop()
        return out

    def __iter__(self):
        return self

    def __next__(self):
        if self.source is not None:
            self.update(self.source.__next__())

    def process_source(self):
        _ = [x for x in self]
        return True

    def __del__(self):
        self._event_loop.close()
        del self._event_loop

        if self.source is not None:
            del self.source
