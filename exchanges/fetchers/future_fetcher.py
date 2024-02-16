import json
import logging

import ccxt
import pandas as pd


class FutureFetcher:
    def __init__(self, exchange):
        self.exchange = exchange

    def fetch_market_data_okx(
        self, market_symbols: list[str]
    ) -> pd.DataFrame:
        print("Fetching data from OKX for futures")
        data_list = []
        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            with open("okx_raw_data_futures.json", "w") as f:
                json.dump(all_tickers, f, indent=4)
        except Exception as e:
            logging.error(f"Error fetching tickers: {e}")

        return pd.DataFrame(data_list)

    def fetch_market_data_binance(
        self, market_symbols: list[str]
    ) -> pd.DataFrame:
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

    def fetch_market_data_deribit(
        self, market_symbols: list[str]
    ) -> pd.DataFrame:
        data_list = []
        try:
            all_tickers = self.exchange.fetch_tickers(market_symbols)
            with open("deribit_raw_data_futures.json", "w") as f:
                json.dump(all_tickers, f, indent=4)
        except Exception as e:
            logging.error(f"Error fetching tickers: {e}")

        return pd.DataFrame(data_list)
