import math
from datetime import datetime


class DataFilter:
    @staticmethod
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

    @staticmethod
    def calculate_implied_forward_price(self, call_data, put_data):
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
        self, option_data_list
    ):
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
    def calculate_time_to_maturity(self, option_order_books_data):
        try:
            symbol = option_order_books_data.get("symbol")

            if symbol is None:
                return 0

            expiration_date_str = symbol.split("-")[1]
            date_format = "%y%m%d" if len(
                expiration_date_str) == 6 else "%d%m%Y"

            current_date = datetime.utcnow()
            current_date = datetime.strptime(
                current_date.strftime(
                    "%Y-%m-%d %H:%M:%S.%f"), "%Y-%m-%d %H:%M:%S.%f"
            )
            expiration_date = datetime.strptime(
                expiration_date_str, date_format)

            time_to_maturity_seconds = (
                expiration_date - current_date).total_seconds()
            time_to_maturity_days = time_to_maturity_seconds / (24 * 3600)
            time_to_maturity_years = time_to_maturity_days / 365.0

            return max(time_to_maturity_years, 0)

        except Exception as e:
            self._handle_error("Error calculating time to maturity", e)
            return 0
