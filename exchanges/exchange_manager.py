import json

import ccxt
import logging

import pandas as pd

from exchanges.fetchers.binance_fetcher import BinanceFetcher
from exchanges.handlers.merge import MergeMarketHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExchangeManager:
    def __init__(self, exchange_id, pairs_to_load, market_types):
        self.exchange_id = exchange_id
        self.pairs_to_load = pairs_to_load
        self.market_types = market_types
        self.exchange = getattr(ccxt, exchange_id)()
        self.binance_fetcher = BinanceFetcher()
        self.merge_market_handler = MergeMarketHandler(self.exchange, market_types)
        self.options_data = pd.DataFrame()
        self.futures_data = pd.DataFrame()

    def fetch_binance_symbols(self):
        binance_option_symbols = self.binance_fetcher.fetch_options_symbols()
        return binance_option_symbols

    def load_specific_pairs(self) -> pd.DataFrame:
        try:
            if self.exchange_id == "binance":
                binance_option_symbols = self.fetch_binance_symbols()
                data = {
                    "BTC/USD:BTC": {
                        "option": binance_option_symbols,
                        "future": None,
                    }
                }
                return self.handle_market_type(data)

            all_markets = self.exchange.load_markets()
            markets_df = pd.DataFrame(all_markets).T
            filtered_markets = self.filter_markets(markets_df)
            return self.handle_market_type(filtered_markets)
        except Exception as e:
            logger.error(f"Error loading specific pairs: {e}")
            return pd.DataFrame()

    def filter_markets(self, markets_df: pd.DataFrame) -> dict:
        filtered_markets = {}
        for pair in self.pairs_to_load:
            base, quote = pair.split(":")[0].split("/")
            for market_type in self.market_types:
                filtered_df = markets_df[
                    (markets_df["base"] == base)
                    & (markets_df["quote"] == quote)
                    & (markets_df["type"] == market_type)
                ]
                symbols = filtered_df["symbol"].tolist()
                if pair not in filtered_markets:
                    filtered_markets[pair] = {}
                filtered_markets[pair][market_type] = symbols
        return filtered_markets

    def handle_market_type(self, loaded_markets: dict) -> pd.DataFrame:
        dataframe = None
        for pair in self.pairs_to_load:
            future_symbols = loaded_markets.get(pair, {}).get("future", [])
            option_symbols = loaded_markets.get(pair, {}).get("option", [])
            dataframe = self.merge_market_handler.handle(option_symbols, future_symbols)

        return dataframe
