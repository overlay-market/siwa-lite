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

    def handle(self) -> pd.DataFrame:
        future_symbols = self.data_fetcher.fetch_future_market_symbols("BTC")
        return self.data_fetcher.fetch_all_implied_interest_rates(future_symbols)
