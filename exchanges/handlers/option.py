import logging

from typing import List

import pandas as pd

from exchanges.fetchers.option_fetcher import OptionFetcher

from exchanges.processing import Processing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptionMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = OptionFetcher(exchange)
        self.processing = Processing()

    def handle(self, market_symbols: List[str]) -> pd.DataFrame:
        market_data = self.data_fetcher.fetch_market_data(
            market_symbols, str(self.exchange)
        )
        market_data.to_json("option_market_data.json", orient="records", indent=4)

        return market_data
