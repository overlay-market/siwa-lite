import logging
from typing import List
import pandas as pd

from exchanges.fetchers.future_fetcher import FutureFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FutureMarketHandler:
    def __init__(self, exchange: str, market_types: List[str]):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = FutureFetcher(exchange)

    def handle(self, market_symbols: List[str]) -> pd.DataFrame:
        if self.exchange == "binance":
            market_data = self.data_fetcher.fetch_market_data_binance(
                self.exchange, market_symbols
            )
        elif self.exchange == "okx":
            market_data = self.data_fetcher.fetch_market_data_okx(
                self.exchange, market_symbols
            )
        elif self.exchange == "deribit":
            market_data = self.data_fetcher.fetch_market_data_deribit(
                self.exchange, market_symbols
            )
        else:
            logger.error(f"Exchange not supported: {self.exchange}")
            return pd.DataFrame()

        return market_data
