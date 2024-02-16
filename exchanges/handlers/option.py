import logging

import ccxt
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
        exchange = getattr(ccxt, self.exchange)()
        if self.exchange == "deribit":
            market_data = self.data_fetcher.fetch_market_data_deribit(
                exchange, market_symbols
            )
        elif self.exchange == "okx":
            market_data = self.data_fetcher.fetch_market_data_okx(
                exchange, market_symbols
            )
        elif self.exchange == "binance":
            market_data = self.data_fetcher.fetch_market_data_binance(
                exchange, market_symbols
            )
        else:
            logger.error(f"Exchange not supported: {self.exchange}")
            return pd.DataFrame()

        return market_data
