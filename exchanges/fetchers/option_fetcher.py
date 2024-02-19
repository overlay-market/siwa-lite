import logging

import numpy as np
import pandas as pd
import requests


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
            tickers_df.to_json(
                f"{exchange_name}_raw_data_options.json", orient="records", indent=4
            )
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
        df["YTM"] = (df["expiry"] - df["datetime"]) / np.timedelta64(1, "Y")

        # Merge the mark prices into the df based on the new 'symbol'
        df = df.merge(mark_prices_df[["symbol", "markPx"]], on="symbol", how="left")

        # Rename the 'markPx' column to 'mark_price' for clarity (optional)
        df.rename(columns={"markPx": "mark_price"}, inplace=True)
        df["mark_price"] = pd.to_numeric(df["mark_price"], errors="coerce").fillna(0.0) * df[
            "underlying_price"
        ]

        # Select and return the desired columns, including the new 'mark_price'
        return df[
            [
                "symbol",
                "bid",
                "ask",
                "mark_price",
                "underlying_price",
                "datetime",
                "expiry",
                "YTM",
            ]
        ]

    def process_binance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Assuming 'info' contains the required details
        print(df)
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

    @staticmethod
    def convert_inst_id_to_symbol(inst_id: str) -> str:
        # Split the instId into its components
        parts = inst_id.split("-")
        currency = f"{parts[0]}/{parts[1]}"  # e.g., BTC/USD
        date = parts[2][:2] + parts[2][2:4] + parts[2][4:]  # Reformat date
        strike_price = parts[3]
        option_type = parts[4]

        # Reassemble into the symbol format
        symbol = f"{currency}:{parts[0]}-{date}-{strike_price}-{option_type}"
        return symbol
