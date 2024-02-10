import json
import logging

import ccxt
from typing import List, Dict

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
        # market_data = self.data_fetcher.fetch_market_data(exchange, market_symbols)
        if self.exchange == "deribit":
            market_data = self.data_fetcher.fetch_market_data_deribit(
                exchange, market_symbols
            )
        elif self.exchange == "okx":
            print("Fetching data from OKX")
            market_data = self.data_fetcher.fetch_market_data_okx(
                exchange, market_symbols
            )
        elif self.exchange == "binance":
            print("Fetching data from Binance")
            market_data = self.data_fetcher.fetch_market_data_binance(
                exchange, market_symbols
            )

        # Calculate implied interest rates

        with_rates = self.processing.calculate_implied_interest_rates(market_data)
        # Build interest rate term structure

        with_rates.to_json("with_rates.json", orient="records", date_format="iso")

        term_structure = self.processing.build_interest_rate_term_structure(with_rates)

        term_structure.to_json(
            "term_structure.json", orient="records", date_format="iso"
        )

        # return term_structure
