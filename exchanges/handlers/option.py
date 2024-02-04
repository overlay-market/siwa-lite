import json
import logging
import pandas as pd
from typing import List, Dict, Optional

from exchanges.data_fetcher import DataFetcher
from exchanges.preprocessing import Preprocessing
from exchanges.consolidate_data import ConsolidateData

from exchanges.constants.utils import SPREAD_MULTIPLIER, SPREAD_MIN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptionMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = DataFetcher(self.exchange)
        self.preprocessing = Preprocessing(self.exchange, self.market_types)
        self.consolidate_data = ConsolidateData(self.exchange)

    def handle(self, market: List[str]) -> List[Dict]:
        index_maturity = 30 / 365  # 30 days in terms of years
        print(f"We have for the option market: {market}")
        order_books = []
        for market_price in market:
            order = self._fetch_and_process_order_book(market_price)
            if order:
                order_books.append(order)
        with open("Option_order_books.json", "w") as f:
            json.dump(order_books, f, indent=4)
        return order_books

    def _fetch_and_process_order_book(self, market_price: Dict) -> Optional[Dict]:
        order = self.data_fetcher.fetch_option_order_books(market_price)
        if not order or not order.get("order_book"):
            return None

        (
            bid_price,
            ask_price,
            mark_price,
            spread,
            mas,
            gms,
        ) = self._calculate_spread_parameters(order)
        if self._is_quote_invalid(bid_price, ask_price, mark_price, spread, mas, gms):
            return None

        order_dict = self._create_order_book_dict(order, mas, gms, spread)
        return order_dict

    @staticmethod
    def _calculate_spread_parameters(order) -> tuple:
        bid_price = order["order_book"]["bids"][0][0]
        ask_price = order["order_book"]["asks"][0][0]
        mark_price = order["mark_price"]
        bid_spread = max(0, mark_price - bid_price)
        ask_spread = max(0, ask_price - mark_price)
        mas = min(bid_spread, ask_spread) * SPREAD_MULTIPLIER
        gms = SPREAD_MIN * SPREAD_MULTIPLIER
        spread = bid_spread + ask_spread
        return bid_price, ask_price, mark_price, spread, mas, gms

    @staticmethod
    def _is_quote_invalid(bid_price, ask_price, mark_price, spread, mas, gms) -> bool:
        return (
            bid_price <= 0
            or ask_price <= 0
            or bid_price > ask_price
            or mark_price <= 0
            or mark_price < bid_price
            or mark_price > ask_price
            or spread > mas
            and spread > gms
        )

    @staticmethod
    def _create_order_book_dict(order, mas, gms, spread) -> Dict:
        df = pd.DataFrame(
            {
                "symbol": [order["symbol"]],
                "bid_price": [order["order_book"]["bids"][0][0]],
                "ask_price": [order["order_book"]["asks"][0][0]],
                "timestamp": [order["order_book"]["timestamp"]],
                "datetime": [order["order_book"]["datetime"]],
                "time_to_maturity_years": [order["time_to_maturity_years"]],
                "mid_price": [order["mid_price"]],
                "mark_price": [order["mark_price"]],
                "mas": [mas],
                "gms": [gms],
                "spread": [spread],
            }
        )
        index_maturity = 30 / 365
        df["option_type"] = df["time_to_maturity_years"].apply(
            lambda x: "near_term" if x <= index_maturity else "next_term"
        )
        df["datetime_readable"] = pd.to_datetime(df["timestamp"], unit="ms").astype(str)
        return df.to_dict(orient="records")[0]

    # Additional method to fetch spot and mark prices if necessary
    def _fetch_prices(self, symbol: str) -> tuple:
        spot_price = self.data_fetcher.fetch_price(symbol, "last")
        mark_price = self.data_fetcher.fetch_mark_price(symbol)
        return spot_price, mark_price
