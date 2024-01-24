from utils import handle_error
import ccxt
import requests
import logging
from constants import BINANCE_API_URL
import time
from consolidate_data import ConsolidateData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFetcher:
    def __init__(self, exchange):
        self.exchange = exchange
        self.consolidate_data = ConsolidateData(self.exchange)

    def fetch_option_order_books(self, symbol: str, limit: int = 100):
        try:
            response = self.exchange.fetch_order_book(symbol, limit=limit)
            standardized_data = self.consolidate_data.standardize_data(symbol, response)
            return standardized_data
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(f"Error fetching order book for symbol '{symbol}'", e)

    def fetch_binance_option_symbols(self):
        try:
            response = requests.get(BINANCE_API_URL)

            if response.status_code == 200:
                exchange_info = response.json()

                for symbol_info in exchange_info.get("optionSymbols", []):
                    if "BTC" in symbol_info.get("symbol"):
                        symbol = symbol_info.get("symbol")
                        print(symbol)
                        data = self.fetch_option_order_books(symbol)
                        time.sleep(5)
                        self.save_data_to_file("binance", data)

            else:
                logger.error(f"Error: {response.status_code}")
                return []
        except (requests.RequestException, Exception) as e:
            self._handle_error("Error fetching Binance option symbols", e)
            return []

    def fetch_future_order_books(self, limit: int = 100):
        try:
            markets = {
                symbol: market
                for symbol, market in self.exchange.markets.items()
                if self.symbol_filter in symbol
            }
            future_markets = {
                symbol: market
                for symbol, market in markets.items()
                if market.get("future", True)
            }

            for symbol, market in future_markets.items():
                response = self.exchange.fetch_order_book(symbol, limit=limit)
                standardized_data = self.consolidate_data.standardize_data(
                    symbol, response
                )
                self.save_data_to_file(self.exchange_name, standardized_data)
                print(standardized_data)

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error("Error fetching future order books", e)

    def fetch_price(self, symbol: str, price_type: str):
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            price = ticker.get(price_type)
            return price
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(f"Error fetching {price_type} for symbol '{symbol}'", e)
            return None

    def fetch_mark_price(self, symbol: str):
        try:
            mark_price = self.fetch_price(symbol, "markPrice") or self.fetch_price(
                symbol, "mark_price"
            )

            if mark_price is not None and mark_price != float("inf"):
                return mark_price

            bid, ask = self.fetch_price(symbol, "bid"), self.fetch_price(symbol, "ask")
            return (bid + ask) / 2 if bid is not None and ask is not None else None

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(f"Error fetching spot price for symbol '{symbol}'", e)
            return None
