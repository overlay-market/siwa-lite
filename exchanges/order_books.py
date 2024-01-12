import ccxt
import requests
import logging
from pprint import pprint
from typing import Optional, List, Dict, Union
import time
import os
import json


''' Configure logging '''
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BINANCE_API_URL = 'https://eapi.binance.com/eapi/v1/exchangeInfo'


class ExchangeManager:
    def __init__(self, exchange_name: str, symbol_filter: Optional[str], market_type: str):
        self.exchange = getattr(ccxt, exchange_name)()
        self.symbol_filter = symbol_filter
        self.exchange_name = exchange_name
        self.market_type = market_type
        self.data: Dict[str,
                        Dict[str, Union[List[float], List[float], float]]] = {}

    def _handle_error(self, error_message: str, exception: Exception):
        logger.error(f"{error_message}: {exception}")

    def initialize_exchange(self):
        try:
            ''' Function to load the markets from exchange '''
            self.exchange.load_markets()
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error initializing exchange '{self.exchange_name}'", e)

    def filter_markets(self) -> List[Dict[str, Union[str, int, float]]]:
        ''' Filtering function, so we take only the symbol which we want from market '''
        try:
            if self.symbol_filter is None:
                return list(self.exchange.markets.values())
            else:
                return [market for market in self.exchange.markets.values() if self.symbol_filter in market['symbol']]
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error filtering markets for exchange '{self.exchange_name}'", e)
            return []

    def _fetch_spot_price(self, symbol: str):
        try:
            ''' Fetch ticker data for the symbol to get the spot price '''
            ticker = self.exchange.fetch_ticker(symbol)
            spot_price = ticker.get('last')
            return spot_price
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error fetching spot price for symbol '{symbol}'", e)
            return None

    def _standardize_data(self, symbol: str, order_book_data: dict) -> dict:
        try:
            ''' Fetch spot price for the symbol '''
            spot_price = self._fetch_spot_price(symbol)

            ''' Create a standardized data dictionary '''
            standardized_data = {
                'symbol': symbol,
                'order_book': order_book_data,
                'current_spot_price': spot_price
            }

            print(standardized_data)

            return standardized_data
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error standardizing data for symbol '{symbol}'", e)
            return {}

    def fetch_option_order_books(self, symbol: str, limit: int = 100):
        try:
            ''' Fetch order book data for a specific symbol '''
            response = self.exchange.fetch_order_book(symbol, limit=limit)
            standardized_data = self._standardize_data(symbol, response)
            return standardized_data
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error fetching order book for symbol '{symbol}'", e)

    def fetch_binance_option_symbols(self):
        try:
            response = requests.get(BINANCE_API_URL)

            if response.status_code == 200:
                exchange_info = response.json()

                for symbol_info in exchange_info.get("optionSymbols", []):
                    if 'BTC' in symbol_info.get("symbol"):
                        symbol = symbol_info.get("symbol")
                        print(symbol)
                        data = self.fetch_option_order_books(symbol)
                        ''' Add a delay between requests to avoid rate limiting '''
                        time.sleep(5)

                        self.save_data_to_file('binance', data)

            else:
                logger.error(f"Error: {response.status_code}")
                return []
        except (requests.RequestException, Exception) as e:
            self._handle_error("Error fetching Binance option symbols", e)
            return []

    def fetch_future_order_books(self, limit: int = 100):
        try:
            markets = {symbol: market for symbol, market in self.exchange.markets.items(
            ) if self.symbol_filter in symbol}
            future_markets = {symbol: market for symbol,
                              market in markets.items() if market.get('future', True)}

            for symbol, market in future_markets.items():
                response = self.exchange.fetch_order_book(symbol, limit=limit)

                standardized_data = self._standardize_data(symbol, response)
                self.save_data_to_file(self.exchange_name, standardized_data)

                pprint(standardized_data)

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error("Error fetching future order books", e)

    def save_data_to_file(self, exchange_name: str, data: dict, filename: str = 'data.txt'):
        try:
            ''' Create a folder if it doesn't exist '''
            folder_name = "data_folder"
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)

            exchange_filename = os.path.join(
                folder_name, f"{exchange_name}_{filename}")

            with open(exchange_filename, 'a') as file:
                file.write(json.dumps(data, indent=2) + '\n')

            logger.info(
                f"Data for {exchange_name} saved to {exchange_filename}")
        except Exception as e:
            self._handle_error("Error saving data to file", e)

    def process_markets(self):
        try:
            ''' Initialize the exchange and filter markets '''
            self.initialize_exchange()

            if self.market_type == 'option':
                markets = self.filter_markets()

                ''' Iterate through filtered markets and fetch order books '''
                for market in markets:
                    if self.exchange_name == 'binance':
                        binance_data = self.fetch_binance_option_symbols()
                        self.save_data_to_file('binance', binance_data)
                    else:
                        symbol = market['symbol']
                        option_order_books_data = self.fetch_option_order_books(
                            symbol)
                        self.save_data_to_file(
                            self.exchange_name, option_order_books_data)

            elif self.market_type == 'future':
                self.fetch_future_order_books()
                ''' Provide the appropriate data to save to the file '''
                future_order_books_data = {}
                self.save_data_to_file(
                    self.exchange_name, future_order_books_data)

            else:
                logging.error("Invalid market type: %s", self.market_type)

        except (ccxt.NetworkError, ccxt.ExchangeError, Exception) as e:
            self._handle_error(
                f"Error processing markets for exchange '{self.exchange_name}'", e)


def main():
    try:
        binance_option = ExchangeManager(
            exchange_name='binance', symbol_filter=None, market_type='option')
        binance_future = ExchangeManager(
            exchange_name='binance', symbol_filter='BTC', market_type='future')
        deribit_option = ExchangeManager(
            exchange_name='deribit', symbol_filter='BTC/USD:BTC', market_type='option')
        deribit_future = ExchangeManager(
            exchange_name='deribit', symbol_filter='BTC/USD:BTC', market_type='future')
        okx_option = ExchangeManager(
            exchange_name='okx', symbol_filter='BTC/USD:BTC', market_type='option')
        okx_future = ExchangeManager(
            exchange_name='okx', symbol_filter='BTC', market_type='future')
        bybit_option = ExchangeManager(
            exchange_name='bybit', symbol_filter='BTC/USDC:USDC', market_type='option')
        bybit_future = ExchangeManager(
            exchange_name='bybit', symbol_filter='BTC', market_type='future')

        logger.info("\nExchange: Binance")
        binance_future.process_markets()
        binance_option.process_markets()

        logger.info("\nExchange: Deribit")
        deribit_future.process_markets()
        deribit_option.process_markets()

        logger.info("\nExchange: OKX")
        okx_future.process_markets()
        okx_option.process_markets()

        logger.info("\nExchange: Bybit")
        bybit_future.process_markets()
        bybit_option.process_markets()

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
