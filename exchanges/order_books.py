import ccxt
import requests
from pprint import pprint


class ExchangeManager:
    def __init__(self, exchange_name, symbol_filter):
        self.exchange_name = exchange_name
        self.symbol_filter = symbol_filter
        self.order_books = {}

    def initialize_exchange(self):
        try:
            # Dynamically create an instance of the specified exchange using ccxt
            self.exchange = getattr(ccxt, self.exchange_name)()
            self.exchange.load_markets()
        except Exception as e:
            print(
                f"An unexpected error occurred while initializing exchange '{self.exchange_name}': {e}")

    def filter_markets(self):
        try:
            # Filter markets based on the provided symbol filter
            if self.symbol_filter is None:
                return list(self.exchange.markets.values())
            else:
                return [market for market in self.exchange.markets.values() if self.symbol_filter in market['symbol']]
        except Exception as e:
            print(
                f"An unexpected error occurred while filtering markets for exchange '{self.exchange_name}': {e}")
            return []

    def fetch_order_books(self, symbol, limit=100):
        try:
            # Fetch order book data for the specified symbol
            response = self.exchange.fetch_order_book(symbol, limit=limit)
            # Store the fetched order book data in the order_books dictionary
            self.order_books[symbol] = response
        except Exception as e:
            print(
                f"Exchange error while fetching order book for symbol '{symbol}': {e}")

    def fetch_binance_option_symbols(self):
        try:
            # Fetch Binance option symbols using a REST API endpoint
            url = "https://eapi.binance.com/eapi/v1/exchangeInfo"
            response = requests.get(url)

            if response.status_code == 200:
                # Parse the response JSON and iterate through option symbols
                exchange_info = response.json()
                for symbol_info in exchange_info.get("optionSymbols", []):
                    if 'BTC' in symbol_info.get("symbol"):
                        # Fetch order books for symbols containing 'BTC'
                        symbol = symbol_info.get("symbol")
                        self.fetch_order_books(symbol)
            else:
                print(f"Error: {response.status_code}")
                pprint(response.text)
        except Exception as e:
            print(
                f"An unexpected error occurred while fetching Binance option symbols: {e}")

    def process_markets(self):
        try:
            # Initialize the exchange and load markets
            self.initialize_exchange()
            # Filter and retrieve markets based on the symbol filter
            markets = self.filter_markets()

            # Process each market
            for market in markets:
                if self.exchange_name == 'binance':
                    # For Binance, fetch option symbols specifically
                    self.fetch_binance_option_symbols()
                else:
                    # For other exchanges, fetch order books for each market symbol
                    symbol = market['symbol']
                    self.fetch_order_books(symbol)

            for symbol, order_book_data in self.order_books.items():
                print(f"Order Book for {symbol}:")
                pprint(order_book_data)

        except Exception as e:
            print(
                f"An unexpected error occurred while processing markets for exchange '{self.exchange_name}': {e}")
