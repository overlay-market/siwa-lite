import pandas as pd

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
        data = BinanceFetcher.get_response(BINANCE_API_OPTIONS_URL+"/eapi/v1/exchangeInfo")["optionSymbols"]
        data_df = pd.DataFrame(data)
        # all symbols with BTC-
        symbols = data_df["symbol"].loc[data_df["symbol"].str.contains("BTC-")]
        return symbols

    @staticmethod
    def fetch_mark_price():
        data = BinanceFetcher.get_response(BINANCE_API_OPTIONS_URL+"/eapi/v1/mark")
        data_df = pd.DataFrame(data)
        # get all where BTC- is in the symbol
        return data_df.loc[data_df["symbol"].str.contains("BTC-")]




    @staticmethod
    def fetch_futures_symbols():
        data = BinanceFetcher.get_response(BINANCE_API_FUTURES_URL)
        if data:
            return [
                res.get("symbol") for res in data if "BTCUSDT_" in res.get("symbol", "")
            ]
        return []