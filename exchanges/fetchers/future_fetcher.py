import json
import logging

import ccxt
import pandas as pd


class FutureFetcher:
    def __init__(self, exchange):
        self.exchange = exchange

    def fetch_future_market_data(
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
            # if exchange_name == "Deribit":
            #     return self.process_deribit_data(tickers_df)
            # elif exchange_name == "OKX":
            #     return self.process_okx_data(tickers_df)
            # elif exchange_name == "Binance":
            #     return self.process_binance_data(tickers_df)
            # else:
            #     logging.error(f"Unsupported exchange: {exchange_name}")
            #     return pd.DataFrame()
            return tickers_df
        except Exception as e:
            logging.error(f"Error fetching tickers from {exchange_name}: {e}")
            return pd.DataFrame()

    def fetch_market_data_okx(self, market_symbols: list[str]) -> pd.DataFrame:
        print("Fetching data from OKX for futures")
        data_list = []
        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            with open("okx_raw_data_futures.json", "w") as f:
                json.dump(all_tickers, f, indent=4)
        except Exception as e:
            logging.error(f"Error fetching tickers: {e}")

        return pd.DataFrame(data_list)

    def fetch_market_data_binance(self, market_symbols: list[str]) -> pd.DataFrame:
        data_list = []
        print(f"Market symbols: {market_symbols}")
        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            print(f"Tickers: {all_tickers}")
            with open("binance_raw_data_futures.json", "w") as f:
                json.dump(all_tickers, f, indent=4)
        except Exception as e:
            logging.error(f"Error fetching tickers: {e}")

        return pd.DataFrame(data_list)

    def fetch_market_data_deribit(self, market_symbols: list[str]) -> pd.DataFrame:
        data_list = []
        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            with open("deribit_raw_data_futures.json", "w") as f:
                json.dump(all_tickers, f, indent=4)
        except Exception as e:
            logging.error(f"Error fetching tickers: {e}")

        return pd.DataFrame(data_list)
