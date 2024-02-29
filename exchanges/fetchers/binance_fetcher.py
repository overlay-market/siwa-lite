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
        mark_prices_options = BinanceFetcher.get_response(
            BINANCE_API_OPTIONS_URL + "/eapi/v1/mark"
        )
        underlying_price = BinanceFetcher.get_response(
            BINANCE_API_SPOT_URL + "/api/v3/ticker/price"
        )
        underlying_price_df = pd.DataFrame(underlying_price)
        mark_prices_options_df = pd.DataFrame(mark_prices_options)
        mark_prices_options_df = mark_prices_options_df.loc[
            mark_prices_options_df["symbol"].str.contains("BTC-")
        ]
        forward_prices = BinanceFetcher.fetch_mark_price_futures()

        # Ensure that only the BTCUSDT price is fetched to match "BTC-" symbols
        ud_price = underlying_price_df.loc[
            underlying_price_df["symbol"] == "BTCUSDT", "price"
        ].iloc[0]

        mark_prices_options_df["underlying_price"] = float(ud_price)
        mark_prices_options_df.rename(columns={"markPrice": "mark_price"}, inplace=True)

        # Convert "mark_price" to float
        mark_prices_options_df["mark_price"] = mark_prices_options_df[
            "mark_price"
        ].astype(float)
        mark_prices_options_df["expiry"] = (
            mark_prices_options_df["symbol"].str.split("-").str[1]
        )
        mark_prices_options_df = mark_prices_options_df.merge(
            forward_prices, on="expiry", how="right"
        )
        # rename symbol_x to symbol
        mark_prices_options_df.rename(columns={"symbol_x": "symbol"}, inplace=True)
        print(mark_prices_options_df.head())

        return mark_prices_options_df[
            ["symbol", "mark_price", "underlying_price", "forward_price"]
        ]

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
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "forward_price": forward_price,
                    "expiry": expiry,
                }
            )

        mark_prices_df = pd.DataFrame(mark_prices)
        return mark_prices_df

    @staticmethod
    def fetch_futures_symbols():
        data = BinanceFetcher.get_response(
            BINANCE_API_FUTURES_URL + "/fapi/v1/premiumIndex"
        )
        if data:
            return [
                res.get("symbol") for res in data if "BTCUSDT_" in res.get("symbol", "")
            ]
        return []
