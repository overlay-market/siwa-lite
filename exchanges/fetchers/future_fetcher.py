from datetime import datetime
import ccxt
import numpy as np
import pandas as pd


class FutureFetcher:
    def __init__(self, exchange):
        self.exchange = exchange

    def fetch_future_market_symbols(self, symbol: str) -> list[str]:
        load_markets = self.exchange.load_markets()
        load_markets_df = pd.DataFrame(load_markets).transpose()
        future_symbols = load_markets_df[
            (load_markets_df["future"] == True)
            & (load_markets_df["symbol"].str.contains(f"{symbol}/USD"))
            & (load_markets_df["symbol"].str.contains(f":{symbol}"))
        ].index.to_list()
        return future_symbols

    def fetch_future_orderbook(self, symbol: str) -> dict:
        order_book = self.exchange.fetch_order_book(symbol)
        bids_df = pd.DataFrame(
            order_book["bids"], columns=["price", "quantity"]
        ).astype({"price": "float"})
        asks_df = pd.DataFrame(
            order_book["asks"], columns=["price", "quantity"]
        ).astype({"price": "float"})
        best_bid = bids_df["price"].max()
        best_ask = asks_df["price"].min()

        forward_price = (best_bid + best_ask) / 2
        expiry = symbol.split("-")[1]
        return {
            "symbol": symbol,
            "forward_price": forward_price,
            "expiry": expiry,
        }

    def fetch_spot_price(self, symbol: str = "BTC/USDT"):
        ticker = self.exchange.fetch_ticker(symbol)
        return ticker["last"]

    def fetch_implied_interest_rate(self, symbol: str) -> dict:
        orderbook = self.fetch_future_orderbook(symbol)
        forward_price = orderbook["forward_price"]
        expiry_str = orderbook["expiry"]

        expiry_date = datetime.strptime(expiry_str, "%y%m%d")  # Corrected format here
        today = datetime.now()
        days_to_expiry = (expiry_date - today).days
        years_to_expiry = days_to_expiry / 365.25

        spot_price = self.fetch_spot_price()

        if years_to_expiry == 0:
            implied_interest_rate = 0
        else:
            implied_interest_rate = (
                np.log(forward_price) - np.log(spot_price)
            ) / years_to_expiry

        return {
            "symbol": symbol,
            "expiry": expiry_str,
            "implied_interest_rate": implied_interest_rate,
            "days_to_expiry": days_to_expiry,
            "years_to_expiry": years_to_expiry,
        }

    def fetch_all_implied_interest_rates(self, symbols: list[str]) -> pd.DataFrame:
        data = [self.fetch_implied_interest_rate(symbol) for symbol in symbols]
        rates_data = pd.DataFrame(data)

        rates_data["expiry"] = pd.to_datetime(rates_data["expiry"], format="%y%m%d")
        # expiry in human readable format
        rates_data["expiry"] = rates_data["expiry"].dt.strftime("%Y-%m-%d")

        return rates_data
