"""
.. module::
    :platform: Unix, Linux, Windows
    :synopsis:

.. moduleauthor:: Will McGinnis <will@predikto.com>

:copyright: (c) 2016 Predikto Inc.
"""

import time
import json
import datetime
import random
from colcleaner.streaming import StreamingColCleaner
from colcleaner.sources.disk import RowJSONDiskSource
from colcleaner.metrics import *

__author__ = 'willmcginnis'


def generate_sample_data():
    with open('sample.json', 'w') as f:
        for _ in range(10000):
            row = {}

            if random.random() > 0.2:
                row['A'] = random.random()
            if random.random() > 0.2:
                row['B'] = random.choice(list('abcdefghijklmnopqrstuvwxyz'))
            if random.random() > 0.2:
                row['C'] = random.random()
            if random.random() > 0.2:
                row['D'] = random.random()
            if random.random() > 0.2:
                row['date'] = datetime.datetime.fromtimestamp(time.time() * random.random()).strftime('%m/%d/%Y %I:%M %p')

            f.write(json.dumps(row) + '\n')

generate_sample_data()

start_time = time.time()

cleaner = StreamingColCleaner(source=RowJSONDiskSource(filename='sample.json'))
cleaner.add_metric_to_all([CountMetric(), TypingMetric()])
cleaner.add_metric('B', [CountMetric(), CardinalityMetric()])
cleaner.add_metric('C', [CardinalityMetric(), MaxMetric()])
cleaner.add_metric('date', DateFormat())

cleaner.process_source()


print(json.dumps(cleaner.report(), indent=4, sort_keys=True))
print(time.time() - start_time)