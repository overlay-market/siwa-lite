from typing import List

import pandas as pd

from exchanges.fetchers.future_fetcher import FutureFetcher
from exchanges.fetchers.option_fetcher import OptionFetcher
from exchanges.processing import Processing


class MergeMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.future_data_fetcher = FutureFetcher(exchange)
        self.options_data_fetcher = OptionFetcher(exchange)
        self.market_types = market_types
        self.processing = Processing()


    """
    Fetches the options and future market data for the given exchange and market types (e.g. "BTC").
    """
    def handle(
        self, options_market: List[str]
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        options_data = self.options_data_fetcher.fetch_market_data(
            options_market, str(self.exchange)
        )
        options_data = self.processing.eliminate_invalid_quotes(options_data)

        """
        First, we fetch the future market symbols for the given exchange and market types (e.g. "BTC").
        Then, we fetch all the implied interest rates for the future market symbols.
        """
        futures_symbols = self.future_data_fetcher.fetch_future_market_symbols("BTC")
        future_data = self.future_data_fetcher.fetch_all_implied_interest_rates(futures_symbols)

        return options_data, future_data
