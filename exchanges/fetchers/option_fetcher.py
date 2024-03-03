import logging
from datetime import time, datetime

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

        df["bid_spread"] = np.maximum(df["mark_price"] - df["bid"], 0)
        df["ask_spread"] = np.maximum(df["ask"] - df["mark_price"], 0)

        df["underlying_price"] = underlying_prices

        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")

        df["expiry"] = df["symbol"].apply(self.date_parser)
        df["strike_price"], df["option_type"] = zip(
            *df["symbol"].apply(self.get_strike_price_and_option_type)
        )
        df["datetime_hum"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["expiry_hum"] = df["expiry"].dt.strftime("%Y-%m-%d %H:%M:%S")
        # Calculate YTM
        df["YTM"] = (df["expiry"] - df["datetime"]) / np.timedelta64(1, "Y")

        # Select and reorder the required columns
        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
                "bid_spread",
                "ask_spread",
                "datetime",
                "expiry",
                "YTM",
                "datetime_hum",
                "expiry_hum",
                "strike_price",
                "option_type",
                "underlying_price",
            ]
        ]

    def process_okx_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Fetch mark prices from OKX API
        response = requests.get(
            "https://www.okx.com/api/v5/public/mark-price?instType=OPTION"
        )
        mark_prices = response.json()["data"]
        mark_prices_df = pd.DataFrame(mark_prices)

        # Convert 'instId' in mark_prices_df to 'symbol' format
        mark_prices_df["symbol"] = mark_prices_df["instId"].apply(
            self.convert_inst_id_to_symbol
        )
        # Continue with the rest of the process, assuming the rest of your method is correct
        df["underlying_price"] = self.exchange.fetch_ticker("BTC/USDT")["last"]
        df["bid"] *= df["underlying_price"]
        df["ask"] *= df["underlying_price"]

        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["expiry"] = df["symbol"].apply(self.date_parser)
        df["datetime_hum"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["expiry_hum"] = df["expiry"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["strike_price"], df["option_type"] = zip(
            *df["symbol"].apply(self.get_strike_price_and_option_type)
        )
        df["YTM"] = (df["expiry"] - df["datetime"]) / np.timedelta64(1, "Y")

        # Merge the mark prices into the df based on the new 'symbol'
        df = df.merge(mark_prices_df[["symbol", "markPx"]], on="symbol", how="left")

        # Rename the 'markPx' column to 'mark_price' for clarity (optional)
        df.rename(columns={"markPx": "mark_price"}, inplace=True)
        df["mark_price"] = (
            pd.to_numeric(df["mark_price"], errors="coerce").fillna(0.0)
            * df["underlying_price"]
        )
        df["bid_spread"] = np.maximum(df["mark_price"] - df["bid"], 0)
        df["ask_spread"] = np.maximum(df["ask"] - df["mark_price"], 0)

        # Select and return the desired columns, including the new 'mark_price'
        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
                "bid_spread",
                "ask_spread",
                "underlying_price",
                "datetime",
                "expiry",
                "datetime_hum",
                "expiry_hum",
                "strike_price",
                "option_type",
                "YTM",
            ]
        ]

    def process_binance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df["symbol"] = df["symbol"].apply(self.convert_usdt_to_usd)
        df["bid"] = df["info"].apply(lambda x: float(x.get("bidPrice", 0)))
        df["ask"] = df["info"].apply(lambda x: float(x.get("askPrice", 0)))

        now = datetime.now()
        df["datetime"] = pd.to_datetime(now)

        df["expiry"] = df["symbol"].apply(self.date_parser)
        df["datetime_hum"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["expiry_hum"] = df["expiry"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["strike_price"], df["option_type"] = zip(
            *df["symbol"].apply(self.get_strike_price_and_option_type)
        )
        df["YTM"] = (df["expiry"] - df["datetime"]) / np.timedelta64(1, "Y")
        df["underlying_price"] = self.binance_fetcher.fetch_spot_price("BTCUSDT")

        forward_prices_df = self.binance_fetcher.fetch_mark_price_futures()
        forward_prices_df['expiry'] = pd.to_datetime(forward_prices_df['expiry'], format='%y%m%d')
        # find all dates between forward_prices_df["expiry"]) in df["expiry"]
        filtered_dates_df = df[df["expiry"].between(forward_prices_df["expiry"].min(), forward_prices_df["expiry"].max())]
        # Now We have in forward_prices_df:
        # symbol  forward_price     expiry
        # 0  BTCUSDT_240628       66702.85 2024-06-28
        # 1  BTCUSDT_240329       63982.80 2024-03-29
        # I want to calculate the forward price for each expiry date in df["expiry"] like for 2024-04-01
        print(forward_prices_df)
        filtered_dates_df.to_json("df.json", orient="records", indent=4)

        df = df.merge(forward_prices_df, on="symbol", how="left")



        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
                "underlying_price",
                "forward_price",
                "bid_spread",
                "ask_spread",
                "datetime",
                "expiry",
                "datetime_hum",
                "expiry_hum",
                "strike_price",
                "option_type",
                "YTM",
            ]
        ]

    @staticmethod
    def date_parser(symbol: str) -> Timestamp | Timestamp | NaTType:
        date_formats = ["%y%m%d", "%d%b%y", "%Y%m%d", "%m%d%Y", "%d%m%Y"]
        parts = symbol.replace(":", "-").replace("/", "-").replace(".", "-").split("-")
        for part in parts:
            if part.isalpha():
                continue
            for date_format in date_formats:
                try:
                    return pd.to_datetime(part, format=date_format)
                except ValueError:
                    continue
        return pd.NaT

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
