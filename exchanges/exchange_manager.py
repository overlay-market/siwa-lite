import json
from typing import List, Dict, Any

import ccxt

from constants.utils import DEBUG_LIMIT
from preprocessing import Preprocessing
from handlers.future import FutureMarketHandler
from handlers.option import OptionMarketHandler
from handlers.spot import SpotMarketHandler

import logging

# Configure logging to display informational messages
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExchangeManager:
    def __init__(self, exchange_id, pairs_to_load, market_types):
        self.pairs_to_load = self._ensure_list(
            pairs_to_load
        )  # Filter criteria for symbols (e.g., 'BTC/USDT' or ['BTC/USDT', 'ETH/USDT'])
        self.exchange_id = exchange_id  # Name of the exchange (e.g., 'binance')
        self.market_types = market_types # Type of market option or future or spot
        self.exchange = getattr(ccxt, self.exchange_id)()
        self.spot_market_handler = SpotMarketHandler()
        self.future_market_handler = FutureMarketHandler()
        self.option_market_handler = OptionMarketHandler(self.exchange, self.market_types)
        self.preprocessing = Preprocessing(self.exchange, self.market_types)

    def _ensure_list(self, item):
        """
        Convert item to a list if it's not already.
        Examples:
        _ensure_list('BTC/USDT') -> ['BTC/USDT']
        so we just convert "BTC/USDT" to ['BTC/USDT'] so that we can iterate over it later
        """

        return [item] if isinstance(item, str) else item

    def load_specific_pairs(self) -> Dict[str, Any]:
        if not self.exchange:
            return {}

        self.exchange.load_markets()

        processed_count = 0
        selected_pairs = {}
        for market_symbol, market in self.exchange.markets.items():
            if DEBUG_LIMIT and processed_count >= DEBUG_LIMIT:
                break  # Stop processing if the debug limit is reached

            if any(
                market_symbol.startswith(pair_prefix)
                for pair_prefix in self.pairs_to_load
            ):
                if market["type"] in self.market_types:
                    selected_pairs[market_symbol] = market
                    processed_count += 1

        self.handle_market_type(self.market_types, self.pairs_to_load, selected_pairs)

    def handle_market_type(
        self, market_type: str, symbol: str, market: Dict[str, Any]
    ) -> None:
        """
        Route the market to a specific handler function based on its type.

        Parameters:
        market_type (str): The type of the market (e.g., 'spot', 'future', 'option').
        symbol (str): The trading pair symbol (e.g., 'BTC/USDT').
        market (Dict[str, Any]): The market information.
        """
        print(f"Handling {market_type} market: {symbol}")
        if market_type == "spot":
            self.spot_market_handler.handle(symbol, market)
        elif market_type == "future":
            self.future_market_handler.handle(symbol, market)
        elif market_type == "option":
            self.option_market_handler.handle(symbol, market)
        else:
            print(f"Unhandled market type: {market_type}")
