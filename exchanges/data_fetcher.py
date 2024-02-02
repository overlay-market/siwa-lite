import ccxt
import requests
import logging
import time
import json
from consolidate_data import ConsolidateData
from constants.urls import BINANCE_API_URL
from utils import handle_error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFetcher:
    def __init__(self, exchange):
        self.exchange = exchange
        self.consolidate_data = ConsolidateData(self.exchange)

    def _fetch_data_with_error_handling(self, fetch_function, *args):
        try:
            return fetch_function(*args)
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            return None

    def fetch_option_order_books(self, symbol):
        response = self.exchange.fetch_order_book(symbol)
        st = self.consolidate_data.standardize_data(symbol, response)
        return st

    def fetch_binance_option_symbols(self):
        try:
            response = requests.get(BINANCE_API_URL)
            if response.status_code != 200:
                logger.error(f"Error: {response.status_code}")
                return []

            exchange_info = response.json()
            for symbol_info in exchange_info.get("optionSymbols", []):
                if "BTC" in symbol_info.get("symbol"):
                    symbol = symbol_info.get("symbol")
                    data = self.fetch_option_order_books(symbol)
                    if data:
                        self.save_data_to_file("binance", data)
                        time.sleep(5)

            return []

        except requests.RequestException as e:
            handle_error("Error fetching Binance option symbols", e)
            return []

    def fetch_future_order_books(self, limit=100):
        future_markets = self._filter_future_markets()
        for symbol in future_markets:
            standardized_data = self.fetch_option_order_books(symbol, limit)
            if standardized_data:
                self.save_data_to_file(self.exchange.name, standardized_data)

    def _filter_future_markets(self):
        return [
            symbol
            for symbol, market in self.exchange.markets.items()
            if market.get("future", False)
        ]

    def fetch_price(self, symbol, price_type):
        return self.exchange.fetch_ticker(symbol)

    def fetch_mark_price(self, symbol):
        mark_price = self.fetch_price(symbol, "markPrice") or self.fetch_price(
            symbol, "mark_price"
        )
        if mark_price is not None and mark_price != float("inf"):
            return mark_price

        bid, ask = self.fetch_price(symbol, "bid"), self.fetch_price(symbol, "ask")
        return (bid + ask) / 2 if bid and ask else None
