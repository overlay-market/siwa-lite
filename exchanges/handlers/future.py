import json
from typing import Dict, Any
from data_fetcher import DataFetcher
import pandas as pd
from utils import handle_error
import math
import numpy as np


class FutureMarketHandler:

    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = DataFetcher(exchange)

    def handle(self, symbol: str, market: Dict[str, Any]):
        return self.fetch_future_order_books(market)

    def fetch_future_order_books(self, market):
        order_filtered = []
        for order in market:
            standardized_data = self.data_fetcher.fetch_option_order_books(order)
            df = pd.DataFrame(
                {
                    "symbol": [standardized_data["symbol"]],
                    "bid_price": [standardized_data["order_book"]["bids"][0][0]],
                    "ask_price": [standardized_data["order_book"]["asks"][0][0]],
                    "timestamp": [standardized_data["order_book"]["timestamp"]],
                    "datetime": [standardized_data["order_book"]["datetime"]],
                    "time_to_maturity_years": [
                        standardized_data["time_to_maturity_years"]
                    ],
                    "mid_price": [standardized_data["mid_price"]],
                    "mark_price": [standardized_data["mark_price"]],
                }
            )

            # Convert DataFrame to dictionary
            yield_curve = self.calculate_yield_curve(df.to_dict(orient="records"))
            df["yield_curve"] = [yield_curve]
            order_book_dict = df.to_dict(orient="records")
            order_book_dict= order_book_dict[0]

            order_filtered.append(order_book_dict)

        return order_filtered

    def calculate_implied_interest_rate(
        self, forward_price, spot_price, time_to_maturity_years
    ):
        try:
            return (
                (math.log(forward_price) - math.log(spot_price))
                / time_to_maturity_years
                if time_to_maturity_years
                else 0
            )
        except ZeroDivisionError:
            return 0

    def interpolate_yield_curve(self, yield_curve):
        interpolated_yield_curve = {}

        for symbol, rates in yield_curve.items():
            # Sort rates based on time_to_maturity_years
            sorted_rates = sorted(rates, key=lambda x: x["time_to_maturity_years"])

            # Extract time_to_maturity_years and implied_interest_rates
            tenors = [rate["time_to_maturity_years"] for rate in sorted_rates]
            rates = [rate["implied_interest_rate"] for rate in sorted_rates]

            # Perform linear interpolation
            interpolated_rates = np.interp(
                np.arange(min(tenors), max(tenors) + 1),
                tenors,
                rates,
                left=rates[0],  # Extrapolate using the first observed rate
                right=rates[-1],  # Extrapolate using the last observed rate
            )

            # Store the results
            interpolated_yield_curve[symbol] = list(interpolated_rates)

        return interpolated_yield_curve

    def calculate_yield_curve(self, option_data_list):
        yield_curve = {}
        try:
            for option_data in option_data_list:
                symbol = option_data.get("symbol")
                forward_price = option_data.get("mark_price")
                spot_price = 3.24  # TODO: Get spot price
                time_to_maturity_years = option_data.get("time_to_maturity_years")

                if (
                    symbol
                    and forward_price is not None
                    and spot_price is not None
                    and time_to_maturity_years is not None
                ):
                    implied_interest_rate = self.calculate_implied_interest_rate(
                        forward_price, spot_price, time_to_maturity_years
                    )

                    if symbol not in yield_curve:
                        yield_curve[symbol] = []
                    yield_curve[symbol].append(
                        {
                            "implied_interest_rate": implied_interest_rate,
                            "time_to_maturity_years": time_to_maturity_years,
                        }
                    )

            return self.interpolate_yield_curve(yield_curve)

        except Exception as e:
            handle_error("Error calculating yield curve", e)
            return {}
