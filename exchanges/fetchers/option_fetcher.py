import json
import logging

import ccxt
import pandas as pd


class OptionFetcher:
    def __init__(self, exchange: str):
        self.exchange = getattr(ccxt, exchange)()

    def fetch_market_data(self, exchange, market_symbols: list[str]) -> pd.DataFrame:
        """
        Args:
            exchange: "deribit" or "bitmex"
            market_symbols: ["BTC-26MAR21-40000-C", "BTC-25JUN21-40000-C", ...]
        Returns:
            pd.DataFrame: DataFrame with market data like
            {
                "symbol": "BTC-26MAR21-40000-C",
                "bid": 0.001,
                "ask": 0.002,
            },
            {
                "symbol": "BTC-25JUN21-40000-C",
                "bid": 0.001,
                "ask": 0.002,
            },
        """
        data_list = []  # Initialize an empty list to store data dictionaries

        for symbol in market_symbols:
            try:
                ticker = exchange.fetch_ticker(symbol)
                info = ticker.get("info", {})  # Access the 'info' dictionary

                bid = ticker.get("bid", 0) if ticker.get("bid") is not None else 0
                ask = ticker.get("ask", 0) if ticker.get("ask") is not None else 0
                mark_price = float(
                    info.get("mark_price", 0)
                )  # Assuming mark_price is always provided
                timestamp = info.get(
                    "timestamp", 0
                )  # Assuming timestamp is always provided
                underlying_price = float(info.get("underlying_price", 0))
                open_interest = float(info.get("open_interest", 0))
                volume = float(
                    info.get("volume", 0)
                )  # Assuming this is the volume in the 'stats' if provided
                greeks = info.get("greeks", {})

                # Extend the dictionary with the new fields
                data_dict = {
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                    "mark_price": mark_price,
                    "timestamp": timestamp,
                    "underlying_price": underlying_price,
                    "open_interest": open_interest,
                    "volume": volume,
                    "greeks": greeks,
                }
                data_list.append(data_dict)  # Append the dictionary to the list

            except Exception as e:
                logging.error(f"Error fetching data for {symbol}: {e}")
                continue
        with open("market_data.json", "w") as f:
            json.dump(data_list, f, indent=4)

        return pd.DataFrame(data_list)
