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
    ) -> pd.DataFrame:
        options_data = self.option_market_handler.handle(options_market)
        if future_market:
            futures_data = self.future_market_handler.handle(future_market)
            merged_data = pd.concat([options_data, futures_data], ignore_index=True)
        else:
            merged_data = options_data

        valid_quotes = self.processing.eliminate_invalid_quotes(merged_data)
        implied_interest_rates = self.processing.calculate_implied_interest_rates(
            valid_quotes
        )
        implied_interest_rates.to_json(
            f"{self.exchange}_implied_interest_rates.json", orient="records", indent=4
        )

        return implied_interest_rates
