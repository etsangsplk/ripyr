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
from colcleaner.sources.disk import CSVDiskSource
from colcleaner.metrics import *

__author__ = 'willmcginnis'


def generate_sample_data():
    with open('sample.csv', 'w') as f:
        f.write('A,B,C,D,date\n')
        for _ in range(10000):
            f.write('%s,%s,%s,%s,%s\n' % (
                random.random(),
                random.choice(list('abcdefghijklmnopqrstuvwxyz')),
                random.random(),
                random.random(),
                datetime.datetime.fromtimestamp(time.time() * random.random()).strftime('%m/%d/%Y %I:%M %p')
            ))

generate_sample_data()

start_time = time.time()

cleaner = StreamingColCleaner(source=CSVDiskSource(filename='sample.csv'))
cleaner.add_metric_to_all(CountMetric())
cleaner.add_metric('B', [CountMetric(), CardinalityMetric()])
cleaner.add_metric('C', [CardinalityMetric(), MaxMetric()])
cleaner.add_metric('date', DateFormat())

cleaner.process_source()


print(json.dumps(cleaner.report(), indent=4, sort_keys=True))
print(time.time() - start_time)