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
    def fetch_futures_symbols():
        url = f"{BINANCE_API_FUTURES_URL}/fapi/v1/premiumIndex"
        data = BinanceFetcher.get_response(url)
        if data:
            return [
                res["symbol"] for res in data if "BTCUSDT_" in res.get("symbol", "")
            ]
        return []

    @staticmethod
    def fetch_mark_price_futures():
        symbols = BinanceFetcher.fetch_futures_symbols()
        mark_prices = []  # This will hold dictionaries
        for symbol in symbols:
            data = BinanceFetcher.get_response(
                BINANCE_API_FUTURES_URL + f"/fapi/v1/depth?symbol={symbol}"
            )

            bids_df = pd.DataFrame(data["bids"], columns=["price", "quantity"]).astype(
                {"price": "float"}
            )
            asks_df = pd.DataFrame(data["asks"], columns=["price", "quantity"]).astype(
                {"price": "float"}
            )

            # Get maximum bid and minimum ask
            best_bid = bids_df["price"].max()
            best_ask = asks_df["price"].min()

            forward_price = (best_bid + best_ask) / 2
            expiry = symbol.split("_")[1]

            mark_prices.append(
                {
                    "symbol": symbol,
                    "forward_price": forward_price,
                    "expiry": expiry,
                }
            )

        mark_prices_df = pd.DataFrame(mark_prices)
        return mark_prices_df

    @staticmethod
    def fetch_mark_price_options():
        mark_prices_options = BinanceFetcher.get_response(
            BINANCE_API_OPTIONS_URL + "/eapi/v1/mark"
        )
        mark_prices_options_df = pd.DataFrame(mark_prices_options)
        mark_prices_options_df = mark_prices_options_df.loc[
            mark_prices_options_df["symbol"].str.contains("BTC-")
        ]

        return mark_prices_options_df

    @staticmethod
    def fetch_spot_price(symbol: str = "BTCUSDT"):
        spot_price = BinanceFetcher.get_response(
            BINANCE_API_SPOT_URL + f"/api/v3/ticker/price?symbol={symbol}"
        )
        return float(spot_price["price"])
