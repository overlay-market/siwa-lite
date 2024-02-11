from feeds.data_feed import DataFeed
from collections import deque

from apis.unisat import UnisatAPI


class NodeMonkes(DataFeed):
    NAME = 'node_monkes'
    ID = 0
    HEARTBEAT = 180
    DATAPOINT_DEQUE = deque([], maxlen=100)

    COLLECTION_ID = 'nodemonkes'
    FLOOR_PX = "floorPrice"

    @classmethod
    def process_source_data_into_siwa_datapoint(cls):
        '''
            Process data from multiple sources
        '''
        node_monkes_data = UnisatAPI().get_collection_stats(cls.COLLECTION_ID).json()["data"]
        floor_price = node_monkes_data[cls.FLOOR_PX]
        print(floor_price)
        return floor_price

    @classmethod
    def create_new_data_point(cls):
        return cls.process_source_data_into_siwa_datapoint()
