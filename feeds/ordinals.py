from feeds.data_feed import DataFeed
from collections import deque 

#from apis.unisat import UnisatAPI   
from apis.magic_eden import MagicEdenAPI

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
        #breakpoint()
        #floor_price = node_monkes_data[cls.FLOOR_PX]
        me_floor = MagicEdenAPI().get_floor_price(cls.COLLECTION_ID)
        print(me_floor)
        if cls.DATAPOINT_DEQUE:
            if me_floor is None or me_floor == 0 or me_floor/cls.DATAPOINT_DEQUE[-1] > 1.2 or me_floor/cls.DATAPOINT_DEQUE[-1] < 0.8:
                return cls.DATAPOINT_DEQUE[-1]
        return me_floor
    
    @classmethod
    def create_new_data_point(cls):
        try:
            return cls.process_source_data_into_siwa_datapoint()
        except Exception as e:
            print(f"Error in {cls.NAME} create_new_data_point: {e}")