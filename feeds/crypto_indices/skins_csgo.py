from feeds.data_feed import DataFeed
from collections import deque
import numpy as np

from apis.csgoskins import CSGOSkins as csgoskins
from apis.priceempire import PriceEmpire as priceempire


class SkinsCsgo(DataFeed):
    NAME = "skinscsgo"
    ID = 2
    HEARTBEAT = 180
    DATAPOINT_DEQUE = deque([], maxlen=100)

    @classmethod
    def process_source_data_into_siwa_datapoint(cls):
        """
        Process data from multiple sources
        """
        res = []
        for source in [csgoskins, priceempire]:
            df = source().get_prices_df()
            df = source().agg_data(df, source().QUANTITY_KEY_FOR_AGG)
            caps = source().get_caps(df, k=100)
            market_data = source().get_index(df, caps)
            if market_data is None:
                continue
            res.append(np.sum(market_data))
        if sum(res) == 0:
            return cls.DATAPOINT_DEQUE[-1]  # Should fail if DEQUE is empty
        else:
            # Take average of values from all sources
            return sum(res) / len(res)

    @classmethod
    def create_new_data_point(cls):
        return cls.process_source_data_into_siwa_datapoint()
