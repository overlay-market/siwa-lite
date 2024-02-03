import json
from typing import Dict, Any, List

from preprocessing import Preprocessing
from data_fetcher import DataFetcher
from consolidate_data import ConsolidateData
import ccxt
import logging
import requests
from constants.urls import BINANCE_API_URL
import time
from utils import handle_error
import pandas as pd
import math


SPREAD_MULTIPLIER = 10
SPREAD_MIN = 0.0005

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptionMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = DataFetcher(self.exchange)
        self.preprocessing = Preprocessing(self.exchange, self.market_types)
        self.consolidate_data = ConsolidateData(self.exchange)

    def handle(self, symbol: str, market):

        index_maturity = 30 / 365  # 30 days in terms of years
        order_books = []
        raw_dara = []
        for slice in market:
            order = self.data_fetcher.fetch_option_order_books(slice)
            raw_dara.append(order)

            if order is None or not order:
                continue

            bid_price = order["order_book"]["bids"][0][0]
            ask_price = order["order_book"]["asks"][0][0]
            mark_price = order["mark_price"]

            bid_spread = max(0, mark_price - bid_price)

            ask_spread = max(0, ask_price - mark_price)

            # Calculate MAS (Maximum Allowed Spread)
            mas = min(bid_spread, ask_spread) * SPREAD_MULTIPLIER

            # Calculate GMS (Global Maximum Spread)
            gms = SPREAD_MIN * SPREAD_MULTIPLIER

            spread = bid_spread + ask_spread

            # Eliminate invalid quotes based on specified scenarios
            if (
                    bid_price <= 0
                    or ask_price <= 0
                    or bid_price > ask_price
                    or mark_price <= 0
                    or mark_price < bid_price
                    or mark_price > ask_price
                    or spread > mas and spread > gms
            ):
                continue

            df = pd.DataFrame(
                {
                    "symbol": [order["symbol"]],
                    "bid_price": [order["order_book"]["bids"][0][0]],
                    "ask_price": [order["order_book"]["asks"][0][0]],
                    # 'mark_price': mark_price,
                    "timestamp": [order["order_book"]["timestamp"]],
                    "datetime": [order["order_book"]["datetime"]],
                    # 'underlying_price': [order['current_spot_price']['underlying_price']],
                    "time_to_maturity_years": [order["time_to_maturity_years"]],
                    "mid_price": [order["mid_price"]],
                    "mark_price": [order["mark_price"]],
                }
            )

            time_to_maturity_years = df["time_to_maturity_years"].iloc[0]

            # Add option_type column based on time_to_maturity_years
            df["option_type"] = (
                "near_term" if time_to_maturity_years <= index_maturity else "next_term"
            )
            df["mas"] = mas
            df["gms"] = gms
            df["spread"] = spread
            df["datetime_readable"] = pd.to_datetime(df["timestamp"], unit="ms").astype(
                str
            )

            # Convert DataFrame to dictionary
            order_book_dict = df.to_dict(orient="records")
            order_book_dict = order_book_dict[0]

            order_books.append(order_book_dict)


        return order_books

    def _fetch_prices(self, symbol):
        # Fetch and return the spot price and the mark price for the symbol.
        spot_price = self.data_fetcher.fetch_price(symbol, "last")
        mark_price = self.data_fetcher.fetch_mark_price(symbol)
        return spot_price, mark_price
