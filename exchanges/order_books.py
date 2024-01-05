import ccxt
import requests
import logging
from pprint import pprint
import time


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
BINANCE_API_URL = 'https://eapi.binance.com/eapi/v1/exchangeInfo'

class ExchangeManager:
    def __init__(self, exchange_name, symbol_filter):
        self.exchange = getattr(ccxt, exchange_name)()
        self.symbol_filter = symbol_filter
        self.exchange_name = exchange_name
        self.order_books = {}

    def initialize_exchange(self):
        try:
            # Load markets for the instantiated exchange
            self.exchange.load_markets()
        except Exception as e:
            logger.error(f"An unexpected error occurred while initializing exchange '{self.exchange_name}': {e}")

    def filter_markets(self):
        try:
            if self.symbol_filter is None:
                # If symbol_filter is None, return all markets
                return list(self.exchange.markets.values())
            else:
                # Filter markets based on the specified symbol filter
                return [market for market in self.exchange.markets.values() if self.symbol_filter in market['symbol']]
        except Exception as e:
            logger.error(f"An unexpected error occurred while filtering markets for exchange '{self.exchange_name}': {e}")
            return []

    def fetch_order_books(self, symbol, limit=100):
        try:
            # Fetch order book data for a specific symbol
            response = self.exchange.fetch_order_book(symbol, limit=limit)
            self.order_books[symbol] = response
            pprint(response)
        except Exception as e:
            logger.error(f"Exchange error while fetching order book for symbol '{symbol}': {e}")

    def fetch_binance_option_symbols(self):
        try:
            response = requests.get(BINANCE_API_URL)

            if response.status_code == 200:
                exchange_info = response.json()
                # Access the "optionSymbols" array and log only the "symbol" attribute for each item
                for symbol_info in exchange_info.get("optionSymbols", []):
                    if 'BTC' in symbol_info.get("symbol"):
                        symbol = symbol_info.get("symbol")
                        self.fetch_order_books(symbol)
                        time.sleep(5)
            else:
                logger.error(f"Error: {response.status_code}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching Binance option symbols: {e}")

    def process_markets(self):
        try:
            # Initialize the exchange and filter markets
            self.initialize_exchange()
            markets = self.filter_markets()

            # Iterate through filtered markets and fetch order books
            for market in markets:
                if self.exchange_name == 'binance':
                    self.fetch_binance_option_symbols()
                else:
                    symbol = market['symbol']
                    self.fetch_order_books(symbol)

        except Exception as e:
            logger.error(f"An unexpected error occurred while processing markets for exchange '{self.exchange_name}': {e}")


def main():
    try:
        manager1 = ExchangeManager(exchange_name='bybit', symbol_filter='BTC/USDC:USDC')
        manager2 = ExchangeManager(exchange_name='okx', symbol_filter='BTC/USD:BTC')
        manager3 = ExchangeManager(exchange_name='deribit', symbol_filter='BTC/USD:BTC')
        manager4 = ExchangeManager(exchange_name='binance', symbol_filter=None)

        logger.info("Exchange: Bybit")
        manager1.process_markets()

        # logger.info("\nExchange: OKX")
        # manager2.process_markets()

        # logger.info("\nExchange: Deribit")
        # manager3.process_markets()

        # logger.info("\nExchange: Binance")
        # manager4.process_markets()
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
