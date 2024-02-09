import json
from typing import List, Any, Dict
import ccxt
import logging

import pandas as pd

from exchanges.fetchers.binance_fetcher import BinanceFetcher
from exchanges.handlers.future import FutureMarketHandler
from exchanges.handlers.option import OptionMarketHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExchangeManager:
    def __init__(
        self, exchange_id: str, pairs_to_load: List[str], market_types: List[str]
    ):
        self.pairs_to_load = pairs_to_load
        self.exchange_id = exchange_id
        self.market_types = market_types
        self.exchange = getattr(ccxt, exchange_id)()
        self.option_market_handler = OptionMarketHandler(exchange_id, market_types)
        # self.future_market_handler = FutureMarketHandler()
        self.binance_fetcher = BinanceFetcher()
        self.options_data = pd.DataFrame()
        self.futures_data = {}

    def load_specific_pairs(self) -> pd.DataFrame:
        """Load and process specific pairs based on predefined filters using Pandas, with output limited to specific base market pairs.
        input: pairs_to_load: List[str] like ["BTC/USD:BTC", "ETH/USD:ETH"], market_types: List[str] like ["future", "option"]
        Output for specified pairs will look like:
        {
            "BTC/USD:BTC": {
                "future": ["BTC-26MAR21", "BTC-25JUN21", ...],
                "option": ["BTC-26MAR21-40000-C", "BTC-25JUN21-40000-C", ...]
            },
            "ETH/USD:ETH": {
                "future": ["ETH-26MAR21", "ETH-25JUN21", ...],
                "option": ["ETH-26MAR21-40000-C", "ETH-25JUN21-40000-C", ...]
            },
            ...
        }
        """
        print(f"Loading specific pairs for {self.exchange_id}")
        try:
            # Load all markets from the exchange
            if self.exchange_id == "binance":
                print("Fetching binance option symbols")
                binance_option_symbols = self.binance_fetcher.fetch_symbols("options")
                data = {"BTC/USD:BTC": {"option": binance_option_symbols}}
                return self.handle_market_type(data)

            all_markets = self.exchange.load_markets()
            # Convert markets data to a pandas DataFrame for easier filtering
            markets_df = pd.DataFrame(all_markets).T  # Transpose to get markets as rows

            # Filter for specified pairs
            filtered_markets = {}
            for pair in self.pairs_to_load:
                base, quote = pair.split(":")[0].split("/")
                for market_type in self.market_types:
                    filtered_df = markets_df[
                        (markets_df["base"] == base)  # if base is BTC (it means from)
                        & (
                            markets_df["quote"] == quote
                        )  # if quote is USD (it means to)
                        & (
                            markets_df["type"] == market_type
                        )  # if market_type is future (it means type of market)
                    ]

                    # Extract symbols for the filtered markets
                    symbols = filtered_df["symbol"].tolist()

                    # Organize data into the desired output format
                    if pair not in filtered_markets:
                        filtered_markets[pair] = {}
                    filtered_markets[pair][market_type] = symbols

            # self.handle_market_type(filtered_markets)
            return self.handle_market_type(filtered_markets)

        except Exception as e:
            logger.error(f"Error loading specific pairs: {e}")
            return pd.DataFrame()

    # TODO: fix this bullshit later
    def handle_market_type(self, loaded_markets: Dict[str, Any]) -> pd.DataFrame:
        """Handle the loaded markets based on the market type, either option or future.
        input: loaded_markets: Dict[str, Any] like {
            "BTC/USD:BTC": {
                "future": ["BTC-26MAR21", "BTC-25JUN21", ...],
                "option": ["BTC-26MAR21-40000-C", "BTC-25JUN21-40000-C", ...]
            },
            "ETH/USD:ETH": {
                "future": ["ETH-26MAR21", "ETH-25JUN21", ...],
                "option": ["ETH-26MAR21-40000-C", "ETH-25JUN21-40000-C", ...]
            },
            ...
        }
        Output will be a list of order books for the specified market type.
        """
        for pair in self.pairs_to_load:
            if "option" in loaded_markets[pair]:
                # x = self.options_data[pair] = self.option_market_handler.handle(
                #     loaded_markets[pair]["option"]
                # )
                print(
                    f"Length of loaded markets: {len(loaded_markets[pair]['option'])}"
                )
                self.options_data = self.option_market_handler.handle(
                    loaded_markets[pair]["option"]
                )

        return self.options_data
