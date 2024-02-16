from exchanges.constants.urls import BINANCE_API_OPTIONS_URL, BINANCE_API_FUTURES_URL


import logging
import requests

# Assuming BINANCE_API_OPTIONS_URL and BINANCE_API_FUTURES_URL are defined elsewhere

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BinanceFetcher:
    @staticmethod
    def get_response(url):
        try:
            with requests.Session() as session:
                response = session.get(url)
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(
                        f"Failed to fetch data from {url}: {response.status_code}"
                    )
                    return None
        except Exception as e:
            logger.error(f"Exception occurred while fetching data from {url}: {e}")
            return None

    @staticmethod
    def fetch_options_symbols():
        data = BinanceFetcher.get_response(BINANCE_API_OPTIONS_URL)
        if data:
            return [
                symbol_info.get("symbol")
                for symbol_info in data.get("optionSymbols", [])
                if "BTC" in symbol_info.get("symbol", "")
            ]
        return []

    @staticmethod
    def fetch_futures_symbols():
        data = BinanceFetcher.get_response(BINANCE_API_FUTURES_URL)
        if data:
            return [
                res.get("symbol") for res in data if "BTCUSD" in res.get("symbol", "")
            ]
        return []

    @staticmethod
    def fetch_symbols():
        options = BinanceFetcher.fetch_options_symbols()
        futures = BinanceFetcher.fetch_futures_symbols()
        if options or futures:
            logger.info("Successfully fetched symbols from Binance")
        else:
            logger.error("Failed to fetch symbols from Binance")
        return options, futures
