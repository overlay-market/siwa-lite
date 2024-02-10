import json
from typing import Dict, Any, List
import math
import pandas as pd
import numpy as np

from exchanges.fetchers.future_fetcher import FutureFetcher


class FutureMarketHandler:
    def __init__(self, exchange: str, market_types: List[str]):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = FutureFetcher(exchange)

    def handle(self, market_symbols: List[str]) -> pd.DataFrame:
        if self.exchange == "binance":
            market_data = self.data_fetcher.fetch_market_data_binance(market_symbols)
        elif self.exchange == "okx":
            market_data = self.data_fetcher.fetch_market_data_okx(
                self.exchange, market_symbols
            )
        elif self.exchange == "deribit":
            market_data = self.data_fetcher.fetch_market_data_deribit(market_symbols)

        return market_data
