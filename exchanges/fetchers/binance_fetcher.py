import pandas as pd

from exchanges.constants.urls import (
    BINANCE_API_OPTIONS_URL,
    BINANCE_API_FUTURES_URL,
    BINANCE_API_SPOT_URL,
)

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
        data = BinanceFetcher.get_response(
            BINANCE_API_OPTIONS_URL + "/eapi/v1/exchangeInfo"
        )["optionSymbols"]
        data_df = pd.DataFrame(data)
        # all symbols with BTC-
        symbols = data_df["symbol"].loc[data_df["symbol"].str.contains("BTC-")]
        return symbols.tolist()

    @staticmethod
    def fetch_mark_and_underlying_price():
        mark_prices = BinanceFetcher.get_response(
            BINANCE_API_OPTIONS_URL + "/eapi/v1/mark"
        )
        underlying_price = BinanceFetcher.get_response(
            BINANCE_API_SPOT_URL + "/api/v3/ticker/price"
        )
        underlying_price_df = pd.DataFrame(underlying_price)
        data_df = pd.DataFrame(mark_prices)
        data_df = data_df.loc[data_df["symbol"].str.contains("BTC-")]

        # Ensure that only the BTCUSDT price is fetched to match "BTC-" symbols
        ud_price = underlying_price_df.loc[
            underlying_price_df["symbol"] == "BTCUSDT", "price"
        ].iloc[0]

        data_df["underlying_price"] = float(ud_price)
        data_df.rename(columns={"markPrice": "mark_price"}, inplace=True)

        # Convert "mark_price" to float
        data_df["mark_price"] = data_df["mark_price"].astype(float)

        return data_df[["symbol", "mark_price", "underlying_price"]]

    @staticmethod
    def fetch_futures_symbols():
        data = BinanceFetcher.get_response(BINANCE_API_FUTURES_URL)
        if data:
            return [
                res.get("symbol") for res in data if "BTCUSDT_" in res.get("symbol", "")
            ]
        return []
