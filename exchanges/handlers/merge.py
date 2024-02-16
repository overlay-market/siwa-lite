from typing import List

import pandas as pd
import json

from exchanges.handlers.future import FutureMarketHandler
from exchanges.handlers.option import OptionMarketHandler


class MergeMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.option_market_handler = OptionMarketHandler(exchange, market_types)
        self.future_market_handler = FutureMarketHandler(exchange, market_types)

    def handle(
        self, options_market: List[str], future_market: List[str] | None
    ) -> pd.DataFrame:
        options_data = self.option_market_handler.handle(options_market)
        if future_market:
            futures_data = self.future_market_handler.handle(future_market)
            merged_data = pd.concat([options_data, futures_data], ignore_index=True)
        else:
            merged_data = options_data


        return merged_data
