import math
from datetime import datetime
from exchanges.utils import handle_error


class DataFilter:
    @staticmethod
    def calculate_implied_interest_rate(
        forward_price, spot_price, time_to_maturity_years
    ):
        """
        Calculates the implied interest rate given forward price, spot price, and time to maturity.
        :param forward_price: The forward price of the asset.
        :param spot_price: The current spot price of the asset.
        :param time_to_maturity_years: Time to maturity in years.
        :return: The calculated implied interest rate.
        """
        # Return 0 if time to maturity is 0 to avoid division by zero.
        if time_to_maturity_years == 0:
            return 0
        try:
            # Calculate the implied interest rate using the formula.
            return (
                math.log(forward_price) - math.log(spot_price)
            ) / time_to_maturity_years
        except ZeroDivisionError:
            # Handle any division by zero errors.
            return 0

    @staticmethod
    def calculate_implied_forward_price(call_data, put_data):
        """
        Calculates the implied forward price using call and put option data.
        :param call_data: Data of the call option including mid_price.
        :param put_data: Data of the put option including mid_price.
        :return: The calculated implied forward price.
        """
        call_price = call_data.get("mid_price")
        put_price = put_data.get("mid_price")
        # Return 0 if either call or put prices are not available.
        if call_price is None or put_price is None:
            return 0

        # Extract the strike price from call option order book bids.
        strike_price = float(call_data.get("order_book", {}).get("bids", [])[0][0])
        # Calculate the implied forward price using the formula.
        return strike_price + (call_price - put_price)

    @staticmethod
    def calculate_yield_curve(option_data_list):
        """
        Calculates the yield curve from a list of option data.
        :param option_data_list: List of option data.
        :return: A dictionary representing the yield curve.
        """
        yield_curve = {}
        for option_data in option_data_list:
            symbol = option_data.get("symbol")
            forward_price = option_data.get("mark_price")
            spot_price = option_data.get("current_spot_price")
            time_to_maturity_years = option_data.get("time_to_maturity_years")

            # Skip calculation if any required data is missing.
            if not (symbol and forward_price and spot_price and time_to_maturity_years):
                continue

            # Calculate the implied interest rate for each option.
            implied_interest_rate = DataFilter.calculate_implied_interest_rate(
                forward_price, spot_price, time_to_maturity_years
            )
            # Append the interest rate to the yield curve for the symbol.
            yield_curve.setdefault(symbol, []).append(implied_interest_rate)
        return yield_curve

    @staticmethod
    def calculate_time_to_maturity(option_order_books_data):
        """
        Calculates the time to maturity for an option given its order book data.
        :param option_order_books_data: The order book data of the option.
        :return: Time to maturity in years.
        """
        symbol = option_order_books_data.get("symbol")
        # Return 0 if the symbol is not provided.
        if not symbol:
            return 0

        # Extract expiration date from the symbol and determine its format.
        expiration_date_str = symbol.split("-")[1]
        date_format = "%y%m%d" if len(expiration_date_str) == 6 else "%d%m%Y"

        try:
            # Parse the current and expiration dates.
            current_date = datetime.utcnow()
            expiration_date = datetime.strptime(expiration_date_str, date_format)
            # Calculate time to maturity in seconds, then convert to years.
            time_to_maturity_seconds = (expiration_date - current_date).total_seconds()
            time_to_maturity_years = time_to_maturity_seconds / (24 * 3600 * 365.0)
            return max(time_to_maturity_years, 0)
        except ValueError as e:
            # Handle any errors in date parsing.
            handle_error("Error parsing date format in calculating time to maturity", e)
            return 0
