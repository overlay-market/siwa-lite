from typing import List

import pandas as pd
import json

from exchanges.handlers.future import FutureMarketHandler
from exchanges.handlers.option import OptionMarketHandler
from exchanges.processing import Processing


class MergeMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.option_market_handler = OptionMarketHandler(exchange, market_types)
        self.future_market_handler = FutureMarketHandler(exchange, market_types)
        self.processing = Processing()

    def handle(
        self, options_market: List[str], future_market: List[str] | None
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        options_data = self.option_market_handler.handle(options_market)
        options_data = self.processing.eliminate_invalid_quotes(options_data)
        future_data = self.future_market_handler.handle()

        return options_data, future_data
