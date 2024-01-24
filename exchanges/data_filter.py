import math
from typing import List, Dict, Union
from datetime import datetime


class DataFilter:
    @staticmethod
    def calculate_implied_interest_rate(
        self, forward_price: float, spot_price: float, time_to_maturity_years: float
    ) -> float:
        try:
            return (
                (math.log(forward_price) - math.log(spot_price))
                / time_to_maturity_years
                if time_to_maturity_years
                else 0
            )
        except ZeroDivisionError:
            return 0

    @staticmethod
    def calculate_implied_forward_price(self, call_data: dict, put_data: dict) -> float:
        try:
            call_price = call_data.get("mid_price")
            put_price = put_data.get("mid_price")
            strike_price = float(call_data.get(
                "order_book", {}).get("bids", [])[0][0])

            if call_price is None or put_price is None:
                return 0  # Handle the case where either call or put prices are not available

            # Calculate the implied forward price using the formula: Fimp = K + F × (C − P)
            forward_price = strike_price + (call_price - put_price)

            return forward_price
        except Exception as e:
            self._handle_error("Error calculating implied forward price", e)
            return 0

    @staticmethod
    def calculate_yield_curve(
        self, option_data_list: List[Dict[str, Union[str, int, float]]]
    ) -> Dict[str, List[float]]:
        yield_curve = {}
        try:
            for option_data in option_data_list:
                symbol = option_data.get("symbol")
                forward_price = option_data.get("mark_price")
                spot_price = option_data.get("current_spot_price")
                time_to_maturity_years = option_data.get(
                    "time_to_maturity_years")

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
                    yield_curve[symbol].append(implied_interest_rate)
                    print(implied_interest_rate)

            return yield_curve
        except Exception as e:
            self._handle_error("Error calculating yield curve", e)
            return {}

    @staticmethod
    def calculate_implied_forward_price(self, call_data: dict, put_data: dict) -> float:
        try:
            call_price = call_data.get("mid_price")
            put_price = put_data.get("mid_price")
            strike_price = float(call_data.get(
                "order_book", {}).get("bids", [])[0][0])

            if call_price is None or put_price is None:
                return 0  # Handle the case where either call or put prices are not available

            forward_price = strike_price + \
                strike_price * (call_price - put_price)

            return forward_price
        except Exception as e:
            self._handle_error("Error calculating implied forward price", e)
            return 0