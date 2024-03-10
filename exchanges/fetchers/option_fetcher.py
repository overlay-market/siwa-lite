import logging
from datetime import time, datetime

import ccxt
import numpy as np
import pandas as pd
import requests
from pandas import Timestamp
from pandas._libs import NaTType

from exchanges.fetchers.binance_fetcher import BinanceFetcher


class OptionFetcher:
    def __init__(self, exchange):
        self.exchange = exchange
        self.binance_fetcher = BinanceFetcher()

    def fetch_market_data(
        self, market_symbols: list[str], exchange_name: str
    ) -> pd.DataFrame:
        """
        Fetches market data for a given list of market symbols from a specified exchange and processes it using pandas.
        Args:
            market_symbols: A list of symbols in the format recognized by the exchange.
            exchange_name: String representing the exchange name ('deribit', 'okx', 'binance').
        Returns:
            pd.DataFrame: DataFrame with processed market data for each option contract.
        """
        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            tickers_df = pd.DataFrame(all_tickers).transpose()
            if exchange_name == "Deribit":
                return self.process_deribit_data(tickers_df)
            elif exchange_name == "OKX":
                return self.process_okx_data(tickers_df)
            elif exchange_name == "Binance":
                return self.process_binance_data(tickers_df)
            else:
                logging.error(f"Unsupported exchange: {exchange_name}")
                return pd.DataFrame()
        except Exception as e:
            logging.error(f"Error fetching tickers from {exchange_name}: {e}")
            return pd.DataFrame()

    def process_deribit_data(self, df: pd.DataFrame) -> pd.DataFrame:
        info_df = pd.json_normalize(df["info"])
        df = df.reset_index(drop=True)

        df["bid"] = pd.to_numeric(df["bid"], errors="coerce").fillna(0.0)
        df["ask"] = pd.to_numeric(df["ask"], errors="coerce").fillna(0.0)

        df["mark_price"] = pd.to_numeric(info_df["mark_price"], errors="coerce").fillna(
            0.0
        )

        underlying_prices = pd.to_numeric(
            info_df["underlying_price"], errors="coerce"
        ).fillna(0.0)

        df["bid"] *= underlying_prices
        df["ask"] *= underlying_prices
        df["mark_price"] *= underlying_prices

        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
            ]
        ]

    def process_okx_data(self, df: pd.DataFrame) -> pd.DataFrame:
        response = requests.get(
            "https://www.okx.com/api/v5/public/mark-price?instType=OPTION"
        )
        mark_prices = response.json()["data"]
        mark_prices_df = pd.DataFrame(mark_prices)
        mark_prices_df["symbol"] = mark_prices_df["instId"].apply(
            self.convert_inst_id_to_symbol
        )
        mark_prices_df.rename(columns={"markPx": "mark_price"}, inplace=True)
        df["underlying_price"] = self.exchange.fetch_ticker("BTC/USDT")["last"]
        df["bid"] *= df["underlying_price"]
        df["ask"] *= df["underlying_price"]

        df = df.merge(mark_prices_df[["symbol", "mark_price"]], on="symbol", how="left")
        df["mark_price"] = pd.to_numeric(df["mark_price"], errors="coerce").fillna(0.0)
        df["mark_price"] *= df["underlying_price"]

        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
            ]
        ]

    def process_binance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df["symbol"] = df["symbol"].apply(self.convert_usdt_to_usd)
        df["bid"] = df["info"].apply(lambda x: float(x.get("bidPrice", 0)))
        df["ask"] = df["info"].apply(lambda x: float(x.get("askPrice", 0)))

        mark_price = self.binance_fetcher.fetch_mark_price_options()
        mark_price["symbol"] = mark_price["symbol"].apply(self.transform_symbol_format)
        mark_price.rename(columns={"markPrice": "mark_price"}, inplace=True)
        mark_price["mark_price"] = pd.to_numeric(
            mark_price["mark_price"], errors="coerce"
        ).fillna(0.0)

        df = df.merge(mark_price, on="symbol", how="left")

        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
            ]
        ]

    @staticmethod
    def convert_inst_id_to_symbol(inst_id: str) -> str:
        parts = inst_id.split("-")
        currency = f"{parts[0]}/{parts[1]}"  # e.g., BTC/USD
        date = parts[2][:2] + parts[2][2:4] + parts[2][4:]  # Reformat date
        strike_price = parts[3]
        option_type = parts[4]

        symbol = f"{currency}:{parts[0]}-{date}-{strike_price}-{option_type}"
        return symbol

    @staticmethod
    def transform_symbol_format(symbol):
        parts = symbol.split("-")
        return f"{parts[0]}/USD:USD-{parts[1]}-{parts[2]}-{parts[3]}"

    @staticmethod
    def convert_usdt_to_usd(symbol: str) -> str:
        parts = symbol.split(":")
        converted_parts = [part.replace("USDT", "USD") for part in parts]
        converted_symbol = ":".join(converted_parts)
        return converted_symbol

    @staticmethod
    def get_strike_price_and_option_type(symbol: str) -> tuple[str, str]:
        parts = symbol.split("-")
        strike_price = parts[-2]
        option_type = parts[-1]
        return strike_price, option_type


if __name__ == "__main__":
    exchange = ccxt.okx()
    option_fetcher = OptionFetcher(exchange)
    market_symbols = ["BTC-240329-15000-P", "BTC-240329-20000-C"]
    exchange_name = "OKX"
    option_fetcher.fetch_market_data(market_symbols, exchange_name)
    print("Market data fetched and processed successfully!")
