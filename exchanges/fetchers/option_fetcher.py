import json
import logging

import numpy as np
import pandas as pd


class OptionFetcher:
    def __init__(self, exchange):
        self.exchange = exchange

    def fetch_market_data_deribit(self, market_symbols: list[str]) -> pd.DataFrame:
        """
        Fetches market data for a given list of market symbols from deribit exchange.
        Args:
            market_symbols: A list of symbols in the format recognized by OKX (e.g., "BTC-USD-240628-42000-C")
        Returns:
            pd.DataFrame: DataFrame with market data for each option contract.
        """
        data_list = []  # Initialize an empty list to store data dictionaries

        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            for symbol, ticker in all_tickers.items():
                info = ticker.get("info", {})
                bid_raw = ticker.get("bid", 0) if ticker.get("bid") is not None else 0
                ask_raw = ticker.get("ask", 0) if ticker.get("ask") is not None else 0
                underlying_price = float(info.get("underlying_price", 0))
                bid = float(bid_raw) * underlying_price
                ask = float(ask_raw) * underlying_price
                mark_price_raw = float(info.get("mark_price", 0))
                mark_price = mark_price_raw * underlying_price
                timestamp = ticker.get("timestamp", 0)
                datetime = pd.to_datetime(timestamp, unit="ms")
                expiry = self.date_parser(symbol)
                ytm = (expiry - datetime) / np.timedelta64(1, "Y")
                estimated_delivery_price = float(
                    info.get("estimated_delivery_price", 0)
                )

                data_dict = {
                    "symbol": symbol,
                    "bid_btc": bid_raw,
                    "ask_btc": ask_raw,
                    "underlying_price": underlying_price,
                    "bid": bid,
                    "ask": ask,
                    "mark_price_btc": mark_price_raw,
                    "mark_price": mark_price,
                    "timestamp": timestamp,
                    "datetime": datetime,
                    "expiry": expiry,
                    "YTM": ytm,
                    "forward_price": estimated_delivery_price,
                }
                data_list.append(data_dict)

        except Exception as e:
            logging.error(f"Error fetching tickers: {e}")

        return pd.DataFrame(data_list)

    def fetch_market_data_okx(self, market_symbols: list[str]) -> pd.DataFrame:
        """
        Fetches market data for a given list of market symbols from OKX exchange.
        Args:
            market_symbols: A list of symbols in the format recognized by OKX (e.g., "BTC-USD-240628-42000-C")
        Returns:
            pd.DataFrame: DataFrame with market data for each option contract.
        """
        data_list = []  # Initialize an empty list to store data dictionaries

        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            for symbol, ticker in all_tickers.items():
                bid = (
                    float(ticker.get("bid", 0)) if ticker.get("bid") is not None else 0
                )
                ask = (
                    float(ticker.get("ask", 0)) if ticker.get("ask") is not None else 0
                )
                timestamp = ticker.get("timestamp", 0)
                datetime = pd.to_datetime(timestamp, unit="ms")
                expiry = self.date_parser(symbol)
                ytm = (expiry - datetime) / np.timedelta64(1, "Y")

                # Construct a dictionary for each symbol with the required data
                data_dict = {
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                    "timestamp": timestamp,
                    "datetime": datetime,
                    "expiry": expiry,
                    "YTM": ytm,
                }
                data_list.append(data_dict)

            with open("okx_data.json", "w") as f:
                json.dump(data_list, f, indent=4)

            with open("okx_raw_data.json", "w") as f:
                json.dump(all_tickers, f, indent=4)

        except Exception as e:
            logging.error(f"Error fetching tickers from OKX: {e}")

        return pd.DataFrame(data_list)

    def fetch_market_data_binance(self, market_symbols: list[str]) -> pd.DataFrame:
        """
        Fetches market data for a given list of market symbols from Binance exchange.
        Args:
            market_symbols: A list of symbols in the format recognized by Binance (e.g., "BTC-240628-29000-P")
        Returns:
            pd.DataFrame: DataFrame with market data for each option contract.
        """
        data_list = []  # Initialize an empty list to store data dictionaries

        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            for symbol, ticker in all_tickers.items():
                info = ticker.get("info", {})
                bid = float(info.get("bidPrice", 0))
                ask = float(info.get("askPrice", 0))
                exercise_price = float(info.get("exercisePrice", 0))
                timestamp = ticker.get("timestamp", 0)  # Timestamp

                # Construct a dictionary for each symbol with the required data
                data_dict = {
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                    "timestamp": timestamp,
                    "underlying-asset": exercise_price,
                }
                data_list.append(data_dict)
            with open("binance_data.json", "w") as f:
                json.dump(data_list, f, indent=4)

            with open("binance_raw_data.json", "w") as f:
                json.dump(all_tickers, f, indent=4)

        except Exception as e:
            print(f"Error fetching tickers from Binance: {e}")

        return pd.DataFrame(data_list)

    @staticmethod
    def date_parser(symbol):
        # Define a list of possible date formats to try
        date_formats = [
            "%y%m%d",  # YYMMDD
            "%d%b%y",  # DDMMMYY
            "%Y%m%d",  # YYYYMMDD
            "%m%d%Y",  # MMDDYYYY
            "%d%m%Y",  # DDMMYYYY
        ]

        # Split the symbol based on common separators
        parts = symbol.replace(":", "-").replace("/", "-").replace(".", "-").split("-")

        # Loop through the parts to identify potential date segment
        for part in parts:
            # Skip segments that are purely alphabetical, as they likely don't contain date info
            if part.isalpha():
                continue
            # Try to parse each segment with each date format until successful
            for date_format in date_formats:
                try:
                    date = pd.to_datetime(part, format=date_format)
                    return date  # Return the parsed date as soon as a successful parse occurs
                except ValueError:
                    continue  # If parsing fails, try the next format

        return pd.NaT  # Return Not-A-Time if no date could be parsed
