from feeds.data_feed import DataFeed
from collections import deque 
from dataclasses import dataclass
import constants as c
from numpy import random


class Test(DataFeed):
    NAME = 'test'
    ID = 0
    HEARTBEAT = 1
    DATAPOINT_DEQUE = deque([], maxlen=100)

    @classmethod
    def create_new_data_point(cls):
        return random.rand()
