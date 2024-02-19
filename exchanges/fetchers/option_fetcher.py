import json
import logging

import numpy as np
import pandas as pd


class OptionFetcher:
    def __init__(self, exchange):
        self.exchange = exchange

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
        # Convert 'info' column to separate columns for easier manipulation
        info_df = pd.json_normalize(df["info"])
        df = df.reset_index(drop=True)

        # Replace 'None' with 0.0 and convert to float for 'bid' and 'ask'
        df["bid"] = pd.to_numeric(df["bid"], errors="coerce").fillna(0.0)
        df["ask"] = pd.to_numeric(df["ask"], errors="coerce").fillna(0.0)

        # Convert 'mark_price' from info_df to numeric and update in df
        df["mark_price"] = pd.to_numeric(info_df["mark_price"], errors="coerce").fillna(
            0.0
        )

        # Assuming info_df and df are aligned by index after pd.json_normalize
        underlying_prices = pd.to_numeric(
            info_df["underlying_price"], errors="coerce"
        ).fillna(0.0)

        # Adjust 'bid' and 'ask' based on 'underlying_prices'
        df["bid"] *= underlying_prices
        df["ask"] *= underlying_prices
        df["mark_price"] *= underlying_prices

        df["underlying_price"] = underlying_prices

        # Convert timestamp to datetime
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")

        # Efficient expiry date parsing (assuming this can be vectorized or is already efficient)
        df["expiry"] = df["symbol"].apply(self.date_parser)

        # Calculate YTM
        df["YTM"] = (df["expiry"] - df["datetime"]) / np.timedelta64(1, "Y")

        # Select and reorder the required columns
        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
                "datetime",
                "expiry",
                "YTM",
                "underlying_price",
            ]
        ]

    def process_okx_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["expiry"] = df["symbol"].apply(self.date_parser)
        df["YTM"] = (df["expiry"] - df["datetime"]) / np.timedelta64(1, "Y")
        return df[["symbol", "bid", "ask", "datetime", "expiry", "YTM"]]

    def process_binance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Assuming 'info' contains the required details
        df["bid"] = df["info"].apply(lambda x: float(x.get("bidPrice", 0)))
        df["ask"] = df["info"].apply(lambda x: float(x.get("askPrice", 0)))
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["expiry"] = df["symbol"].apply(self.date_parser)
        df["YTM"] = (df["expiry"] - df["timestamp"]) / np.timedelta64(1, "Y")

        return df[["symbol", "bid", "ask", "timestamp", "expiry", "YTM"]]

    @staticmethod
    def date_parser(symbol: str) -> pd.Timestamp:
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
