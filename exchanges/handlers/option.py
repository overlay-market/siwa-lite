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
        if str(self.exchange) == "Deribit":
            market_data = self.data_fetcher.fetch_market_data_deribit(market_symbols)
        elif str(self.exchange) == "OKX":
            market_data = self.data_fetcher.fetch_market_data_okx(market_symbols)
        elif str(self.exchange) == "Binance":
            market_data = self.data_fetcher.fetch_market_data_binance(market_symbols)
        else:
            logger.error(f"Exchange not supported: {self.exchange}")
            return pd.DataFrame()

        return market_data
