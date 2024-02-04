from typing import List, Any, Dict
import ccxt
import logging

from constants.utils import DEBUG_LIMIT
from handlers.merge import MergeMarketHandler
from preprocessing import Preprocessing
from handlers.future import FutureMarketHandler
from handlers.option import OptionMarketHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExchangeManager:
    def __init__(
        self, exchange_id: str, pairs_to_load: List[str], market_types: List[str]
    ):
        self.pairs_to_load = self._ensure_list(pairs_to_load)
        self.exchange_id = exchange_id
        self.market_types = market_types
        self.exchange = getattr(ccxt, exchange_id)()
        self.future_market_handler = FutureMarketHandler(self.exchange, market_types)
        self.option_market_handler = OptionMarketHandler(self.exchange, market_types)
        self.merge_market_handler = MergeMarketHandler()
        self.preprocessing = Preprocessing(self.exchange, market_types)
        self.future_results = None
        self.option_results = None

    @staticmethod
    def _ensure_list(item: Any) -> List[str]:
        """Ensure the input item is a list."""
        return [item] if isinstance(item, str) else item

    def load_specific_pairs(self) -> None:
        """Load and process specific pairs based on predefined filters.
        Output will look like:
        {
            "future": ["BTC-26MAR21", "BTC-25JUN21", ...],
            "option": ["BTC-26MAR21-40000-C", "BTC-25JUN21-40000-C", ...]
        }
        """
        try:
            self.exchange.load_markets()

            result = {market_type: [] for market_type in self.market_types}
            for market_type in self.market_types:
                for symbol, market in self.exchange.markets.items():
                    if market["type"] == market_type and any(
                        symbol.startswith(pair_prefix)
                        for pair_prefix in self.pairs_to_load
                    ):
                        result[market_type].append(symbol)

                        # Apply DEBUG_LIMIT per market_type if needed
                        if DEBUG_LIMIT and len(result[market_type]) >= DEBUG_LIMIT:
                            break

            self.handle_market_type(result)
        except Exception as e:
            logger.error(f"Failed to load or process pairs: {e}")
            # return {market_type: [] for market_type in self.market_types}
            self.handle_market_type(
                {market_type: [] for market_type in self.market_types}
            )

    def handle_market_type(self, selected_pairs: Dict[str, List[str]]):
        """Handle market type based on predefined filters."""
        try:
            for market_type in self.market_types:
                if market_type == "future":
                    self.future_results = self.future_market_handler.handle(
                        selected_pairs[market_type]
                    )
                elif market_type == "option":
                    self.option_results = self.option_market_handler.handle(
                        selected_pairs[market_type]
                    )
        except Exception as e:
            logger.error(f"Failed to handle market type: {e}")

    def get_results(self) -> Dict[str, Any]:
        """Return the results of handling future and option market types."""
        return {"future": self.future_results, "option": self.option_results}
