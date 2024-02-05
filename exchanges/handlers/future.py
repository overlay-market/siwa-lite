import json
from typing import Dict, Any, List
from exchanges.data_fetcher import DataFetcher
import math
import pandas as pd
import numpy as np


class FutureMarketHandler:
    def __init__(self, exchange: str, market_types: List[str]):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = DataFetcher(exchange)

    def handle(self, market: List[str]) -> List[Dict[str, Any]]:
        # return list(self.fetch_future_order_books(market))
        with open("Future_order_books.json", "w") as f:
            json.dump(list(self.fetch_future_order_books(market)), f, indent=4)
        return list(self.fetch_future_order_books(market))

    def fetch_future_order_books(
        self, market: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        order_filtered = []
        for order in market:
            standardized_data = self.data_fetcher.fetch_option_order_books(order)
            if not standardized_data:
                continue  # Skip if data fetch failed or returned empty data

            yield_curve = self.calculate_yield_curve([standardized_data])
            standardized_data["yield_curve"] = yield_curve

            order_dict = self._create_order_book_dict(standardized_data)
            order_filtered.append(order_dict)

        return order_filtered

    def calculate_implied_interest_rate(
        self, forward_price: float, spot_price: float, time_to_maturity_years: float
    ) -> float:
        try:
            return (
                math.log(forward_price / spot_price) / time_to_maturity_years
                if time_to_maturity_years
                else 0
            )
        except (ZeroDivisionError, ValueError):
            return 0

    def interpolate_yield_curve(
        self, yield_curve: Dict[str, List[Dict[str, float]]]
    ) -> Dict[str, List[float]]:
        interpolated_yield_curve = {}
        for symbol, rates in yield_curve.items():
            sorted_rates = sorted(rates, key=lambda x: x["time_to_maturity_years"])
            tenors = [rate["time_to_maturity_years"] for rate in sorted_rates]
            rates = [rate["implied_interest_rate"] for rate in sorted_rates]

            if tenors and rates:
                interpolated_rates = np.interp(
                    np.arange(min(tenors), max(tenors) + 1),
                    tenors,
                    rates,
                    left=rates[0],
                    right=rates[-1],
                )
                interpolated_yield_curve[symbol] = list(interpolated_rates)

        return interpolated_yield_curve

    def calculate_yield_curve(
        self, option_data_list: List[Dict[str, Any]]
    ) -> Dict[str, List[float]]:
        yield_curve = {}
        for option_data in option_data_list:
            symbol = option_data.get("symbol")
            forward_price = option_data.get("mark_price")
            spot_price = 3.24  # Placeholder for actual spot price fetching logic
            time_to_maturity_years = option_data.get("time_to_maturity_years")

            if None in [symbol, forward_price, spot_price, time_to_maturity_years]:
                continue  # Skip if any essential data is missing

            implied_interest_rate = self.calculate_implied_interest_rate(
                forward_price, spot_price, time_to_maturity_years
            )
            if symbol not in yield_curve:
                yield_curve[symbol] = []
            yield_curve[symbol].append(
                {
                    "implied_interest_rate": implied_interest_rate
                }
            )

        return yield_curve

    @staticmethod
    def _create_order_book_dict(order) -> Dict:
        df = pd.DataFrame(
            {
                "symbol": [order["symbol"]],
                "bid_price": [order["current_spot_price"]["bid"]],
                "ask_price": [order["current_spot_price"]["ask"]],
                "timestamp": [order["order_book"]["timestamp"]],
                "datetime": [order["order_book"]["datetime"]],
                "time_to_maturity_years": [order["time_to_maturity_years"]],
                "mid_price": [order["mid_price"]],
                "mark_price": [order["mark_price"]],
                "yield_curve": [order.get("yield_curve")],
                # "mas": [mas],
                # "gms": [gms],
                # "spread": [spread],
            }
        )
        index_maturity = 30 / 365
        df["option_type"] = df["time_to_maturity_years"].apply(
            lambda x: "near_term" if x <= index_maturity else "next_term"
        )
        df["datetime_readable"] = pd.to_datetime(df["timestamp"], unit="ms").astype(str)
        return df.to_dict(orient="records")[0]
