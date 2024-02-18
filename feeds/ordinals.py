from feeds.data_feed import DataFeed
from collections import deque 
import numpy as np

#from apis.unisat import UnisatAPI   
from apis.magic_eden import MagicEdenAPI
from apis.OKX import OKXAPI

class OrdinalsFeed(DataFeed):
    HEARTBEAT = 180
    DATAPOINT_DEQUE = deque([], maxlen=100)
    FLOOR_PX = "floorPrice"
    CHAIN: 'bitcoin'

    NAME = None # define on child class
    COLLECTION_ID = None # define on child class

    @classmethod
    def process_source_data_into_siwa_datapoint(cls):
        '''
            Process data from multiple sources
        '''
        #node_monkes_data = UnisatAPI().get_collection_stats(cls.COLLECTION_ID).json()["data"]
        #unisat_floor_price = node_monkes_data[cls.FLOOR_PX]

        me_floor = MagicEdenAPI().get_floor_price(cls.MAGIC_EDEN_COLLECTION_ID)
        okx_floor = OKXAPI().get_floor_price(cls.OKX_COLLECTION_ID)
        floor = np.median([me_floor, okx_floor])
        print(cls.NAME, me_floor, okx_floor, floor)   
        if cls.DATAPOINT_DEQUE:
            if floor is None or floor == 0 or floor/cls.DATAPOINT_DEQUE[-1] > 1.2 or floor/cls.DATAPOINT_DEQUE[-1] < 0.8:
                return cls.DATAPOINT_DEQUE[-1]
        return floor
    
    @classmethod
    def create_new_data_point(cls):
        try:
            return cls.process_source_data_into_siwa_datapoint()
        except Exception as e:
            print(f"Error in {cls.NAME} create_new_data_point: {e}")