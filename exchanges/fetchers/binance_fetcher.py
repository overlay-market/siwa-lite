import requests

from exchanges.constants.urls import BINANCE_API_URL
from exchanges.exchange_manager import logger


# Assuming BINANCE_API_URL and logger are defined elsewhere as before.

class BinanceFetcher:
    @staticmethod
    def fetch_symbols(symbol_type):
        """
        Fetch symbols from Binance API.

        Args:
            symbol_type (str): Type of symbols to fetch. Expected values: 'optionSymbols' or 'futuresSymbols'.

        # Example usage:
        # binance_fetcher = BinanceFetcher()
        # option_symbols = binance_fetcher.fetch_symbols('options')
        # futures_symbols = binance_fetcher.fetch_symbols('futures')
        """
        symbol_key = {
            'options': 'optionSymbols',
            'futures': 'futuresSymbols',
        }.get(symbol_type)

        if not symbol_key:
            logger.error(f"Invalid symbol type: {symbol_type}")
            return []

        try:
            with requests.Session() as session:
                response = session.get(BINANCE_API_URL)

            if response.status_code == 200:
                exchange_info = response.json()
                symbols = []

                for symbol_info in exchange_info.get(symbol_key, []):
                    if 'BTC' in symbol_info.get("symbol", ""):
                        symbols.append(symbol_info.get("symbol"))

                return symbols
            else:
                logger.error(f"Error fetching {symbol_type}: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Exception occurred: {e}")
            return []
