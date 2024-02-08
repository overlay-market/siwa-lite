import json
import logging

import ccxt
import pandas as pd


class OptionFetcher:
    def __init__(self, exchange: str):
        self.exchange = getattr(ccxt, exchange)()

    @staticmethod
    def fetch_market_data_deribit(exchange, market_symbols: list[str]) -> pd.DataFrame:
        """
        Fetches market data for a given list of market symbols from deribit exchange.
        Args:
            exchange: An instance of an exchange (here expected to be OKX initialized with ccxt)
            market_symbols: A list of symbols in the format recognized by OKX (e.g., "BTC-USD-240628-42000-C")
        Returns:
            pd.DataFrame: DataFrame with market data for each option contract.
        """
        data_list = []  # Initialize an empty list to store data dictionaries
        raw_data = []  # Initialize an empty list to store raw data

        try:
            all_tickers = exchange.fetch_tickers(market_symbols)
            for symbol, ticker in all_tickers.items():
                info = ticker.get("info", {})
                bid = ticker.get("bid", 0) if ticker.get("bid") is not None else 0
                ask = ticker.get("ask", 0) if ticker.get("ask") is not None else 0
                mark_price = float(info.get("mark_price", 0))
                timestamp = ticker.get("timestamp", 0)
                underlying_price = float(info.get("underlying_price", 0))
                open_interest = float(info.get("open_interest", 0))
                interest_rate = float(info.get("interest_rate", 0))
                volume = float(info.get("volume", 0))
                estimated_delivery_price = float(
                    info.get("estimated_delivery_price", 0)
                )

                data_dict = {
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                    "mark_price": mark_price,
                    "timestamp": timestamp,
                    "underlying_price": underlying_price,
                    "open_interest": open_interest,
                    "interest_rate": interest_rate,
                    "volume": volume,
                    "estimated_delivery_price": estimated_delivery_price,
                }
                data_list.append(data_dict)
                raw_data.append(ticker)

        except Exception as e:
            logging.error(f"Error fetching tickers: {e}")

        return pd.DataFrame(data_list)

    @staticmethod
    def fetch_market_data_okx(exchange, market_symbols: list[str]) -> pd.DataFrame:
        """
        Fetches market data for a given list of market symbols from OKX exchange.
        Args:
            exchange: An instance of an exchange (here expected to be OKX initialized with ccxt)
            market_symbols: A list of symbols in the format recognized by OKX (e.g., "BTC-USD-240628-42000-C")
        Returns:
            pd.DataFrame: DataFrame with market data for each option contract.
        """
        data_list = []  # Initialize an empty list to store data dictionaries

        try:
            all_tickers = exchange.fetch_tickers(market_symbols)
            for symbol, ticker in all_tickers.items():
                info = ticker.get("info", {})
                bid = float(info.get("bidPx", 0))
                ask = float(info.get("askPx", 0))
                last = float(info.get("last", 0))
                bid_volume = float(info.get("bidSz", 0))
                ask_volume = float(info.get("askSz", 0))
                high = float(info.get("high24h", 0))
                low = float(info.get("low24h", 0))
                open_price = float(info.get("open24h", 0))
                volume = float(info.get("vol24h", 0))  # 24h volume
                timestamp = info.get("ts", 0)  # Timestamp

                # Construct a dictionary for each symbol with the required data
                data_dict = {
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                    "last": last,
                    "bid_volume": bid_volume,
                    "ask_volume": ask_volume,
                    "high": high,
                    "low": low,
                    "open": open_price,
                    "volume": volume,
                    "timestamp": timestamp,
                }
                data_list.append(data_dict)

        except Exception as e:
            logging.error(f"Error fetching tickers from OKX: {e}")

        return pd.DataFrame(data_list)
