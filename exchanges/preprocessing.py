from itertools import groupby
from collections import Counter
from datetime import datetime

import numpy as np

from data_fetcher import DataFetcher
from market_filter import MarketFilter
from data_saver import DataSaver
from data_filter import DataFilter


class Preprocessing:
    def __init__(self, exchange_name, symbol_filter):
        self.exchange_name = exchange_name
        self.symbol_filter = symbol_filter
        self.data_fetcher = DataFetcher(self.exchange_name)
        self.market_filter = MarketFilter(self.exchange_name, self.symbol_filter)
        self.data_saver = DataSaver()
        self.data_filter = DataFilter()

    def select_otm_options(
        self, call_data, put_data, katm_strike, implied_forward_price, range_mult
    ):
        # Calculate Kmin and Kmax
        k_min = implied_forward_price / range_mult
        k_max = implied_forward_price * range_mult

        call_strike = float(call_data["order_book"]["bids"][0][0])
        put_strike = float(put_data["order_book"]["asks"][0][0])
        print(f"Call Strike: {call_strike}, Put Strike: {put_strike}")

        # if k_min < call_strike < k_max and k_min < put_strike < k_max:
        #     # If both call and put options are within the specified range, keep them
        #     return call_data, put_data
        # else:
        #     # Otherwise, skip the option that is outside the range
        #     if call_strike < k_min or call_strike > k_max:
        #         call_data["mid_price"] = None  # Skip the call option
        #     if put_strike < k_min or put_strike > k_max:
        #         put_data["mid_price"] = None  # Skip the put option
        #
        # return call_data, put_data

        # TODO - Simplify the above code

        if not (k_min < call_strike < k_max):
            call_data["mid_price"] = None
        if not (k_min < put_strike < k_max):
            put_data["mid_price"] = None

        return call_data, put_data

    def extract_expiry_and_filter_data(self, markets):
        expiry_counts = Counter()
        filtered_data = []

        for market in markets:
            symbol = market.get("symbol")
            option_order_books_data = self.data_fetcher.fetch_option_order_books(symbol)

            if not option_order_books_data:
                print(f"No data fetched for symbol: {symbol}")
                continue

            expiration_date = self.extract_expiration_date(symbol)
            time_to_maturity_years = self.data_filter.calculate_time_to_maturity(
                self, option_order_books_data
            )

            option_order_books_data["option_type"] = (
                "near_term" if time_to_maturity_years <= 30 / 365 else "next_term"
            )

            self.data_saver.save_data(option_order_books_data, self.exchange_name)

            expiry_counts[expiration_date] += 1
            filtered_data.append(option_order_books_data)

        return expiry_counts, filtered_data

    def filter_near_term_options(self, markets):
        return [
            market for market in markets if market.get("option_type") != "near_term"
        ]

    def extract_expiration_date(self, symbol):
        expiration_date_str = symbol.split("-")[1]
        return datetime.strptime(expiration_date_str, "%y%m%d")

    def find_most_common_expiry(self, expiry_counts):
        most_common_expiry = expiry_counts.most_common(1)
        return most_common_expiry[0][0] if most_common_expiry else None

    def filter_data_by_expiry(self, filtered_data, most_common_expiry):
        date_format = "%y%m%d"
        sorted_data = sorted(
            filtered_data, key=lambda x: x.get("order_book", {}).get("bids", [])[0][0]
        )

        grouped_data = {}
        for key, group in groupby(
            sorted_data, key=lambda x: x.get("symbol").split("-")[1]
        ):
            grouped_data[key] = list(group)

        filtered_data_after_threshold = []

        # Set the minimum bid threshold based on tick size
        tick_size = 0.01  # Replace with the actual tick size for your options

        for expiry_date, data_list in grouped_data.items():
            sorted_data_by_strike = sorted(
                data_list, key=lambda x: x.get("order_book", {}).get("bids", [])[0][0]
            )
            consecutive_bids = 0

            for data in sorted_data_by_strike:
                bids = data.get("order_book", {}).get("bids", [])
                if bids:
                    if float(bids[0][0]) <= tick_size:
                        consecutive_bids += 1
                        if consecutive_bids >= 5:
                            break  # Stop processing if five consecutive bids are below or equal to the threshold
                    else:
                        consecutive_bids = 0  # Reset consecutive bids count
                    filtered_data_after_threshold.append(data)

        return filtered_data_after_threshold

    def find_minimum_difference_strike(self, filtered_data):
        min_diff_strike = None
        min_diff_value = float("inf")

        for data in filtered_data:
            bids = data.get("order_book", {}).get("bids", [])
            asks = data.get("order_book", {}).get("asks", [])
            if bids and asks:
                diff_strike = min(
                    zip(bids, asks), key=lambda x: abs(float(x[0][0]) - float(x[1][0]))
                )

                diff_value = abs(float(diff_strike[0][0]) - float(diff_strike[1][0]))

                if diff_value < min_diff_value:
                    min_diff_value = diff_value
                    min_diff_strike = diff_strike
                    print(min_diff_value)

        return min_diff_strike

    def extract_call_put_and_bids_asks(self, min_diff_strike, filtered_data):
        call_data = {
            "mid_price": min_diff_strike[0][0],
            "order_book": {"bids": [min_diff_strike[0]]},
        }
        put_data = {
            "mid_price": min_diff_strike[1][0],
            "order_book": {"asks": [min_diff_strike[1]]},
        }

        bids, asks = [], []

        for data in filtered_data:
            if "bids" in data.get("order_book", {}) and "asks" in data.get(
                "order_book", {}
            ):
                bids += data["order_book"]["bids"]
                asks += data["order_book"]["asks"]

        return call_data, put_data, bids, asks

    def calculate_implied_forward_price(self, call_data, bids, implied_forward_price):
        largest_strike = 0
        for bid in bids:
            if len(bid) >= 2:
                bid_strike = float(bid[0])
                if bid_strike < implied_forward_price and bid_strike > largest_strike:
                    largest_strike = bid_strike

        # TODO - Rewrite using list comprehension
        # largest_strike = max([float(bid[0]) for bid in bids if float(bid[0]) < implied_forward_price], default=0)

        if largest_strike > 0:
            print(
                f"Largest Strike (KATM) for {call_data['order_book']['bids'][0][0]}: {largest_strike}"
            )
        else:
            print("No valid bid-ask pairs found in the filtered data.")

    def calculate_implied_variance(
        self, F_i, K_i_ATM, strikes, option_prices, r_i, T_i, delta_K
    ):
        """
        Calculate the implied variance of near and next term options.

        :param F_i: float, the implied forward price
        :param K_i_ATM: float, the ATM strike level
        :param strikes: list of floats, the strikes K_j
        :param option_prices: list of floats, the option prices V(K_j) for the corresponding strikes
        :param r_i: float, the risk-free interest rate
        :param T_i: float, the time to expiry in years
        :param delta_K: list of floats, the difference between strikes ΔK_j
        :return: float, the implied variance σ_{i,t}^2
        """
        # Precompute constant
        discount_factor = np.exp(r_i * T_i)

        # Vectorized operations
        weights = discount_factor * (delta_K / strikes**2)
        sum_term = np.dot(weights, option_prices)  # Using dot product for weighted sum

        # Calculate the implied variance using the formula
        implied_variance = (1 / T_i) * (2 * sum_term - ((F_i / K_i_ATM) - 1) ** 2)

        return implied_variance

    def interpolate_variance(self, T_NEAR, T_NEXT, T_INDEX):
        """
        Calculate the weights for the near and next term variances based on the given times to maturity.

        Parameters:
        T_NEAR (float): Time to maturity for the near term.
        T_NEXT (float): Time to maturity for the next term.
        T_INDEX (float): Time to maturity for the index.

        Returns:
        tuple: A tuple containing the weights for the near term (omega_NEAR) and the next term (omega_NEXT).
        """
        omega_NEAR_t = (T_NEXT - T_INDEX) / (T_NEXT - T_NEAR) / T_INDEX
        omega_NEXT_t = (T_INDEX - T_NEAR) / (T_NEXT - T_NEAR) / T_NEXT

        return omega_NEAR_t, omega_NEXT_t

    def calculate_raw_implied_variance(
        self, omega_NEAR_t, sigma2_NEAR_t, omega_NEXT_t, sigma2_NEXT_t
    ):
        """
        Calculate the raw value of implied variance at the index maturity.

        Parameters:
        omega_NEAR_t (float): Weight for the near term variance.
        sigma2_NEAR_t (float): Near term variance.
        omega_NEXT_t (float): Weight for the next term variance.
        sigma2_NEXT_t (float): Next term variance.

        Returns:
        float: The raw value of implied variance at the index maturity.
        """
        sigma2_RAW_t = omega_NEAR_t * sigma2_NEAR_t + omega_NEXT_t * sigma2_NEXT_t
        return sigma2_RAW_t

    def calculate_ewma(self, lambda_param, sigma2_SMOOTH_t_minus_1, sigma2_RAW_t):
        """
        Calculate the Exponentially-Weighted Moving Average (EWMA) of raw implied variance.

        Parameters:
        lambda_param (float): The smoothing parameter lambda.
        sigma2_SMOOTH_t_minus_1 (float): The previous value of the smoothed implied variance.
        sigma2_RAW_t (float): The raw implied variance at time t.

        Returns:
        float: The smoothed implied variance at time t.
        """
        sigma2_SMOOTH_t = (
            lambda_param * sigma2_SMOOTH_t_minus_1 + (1 - lambda_param) * sigma2_RAW_t
        )
        return sigma2_SMOOTH_t

    def calculate_ewma_recursive(
        self, lambda_param, tau, sigma2_SMOOTH_previous, sigma2_RAW_history
    ):
        """
        Calculate the Exponentially-Weighted Moving Average (EWMA) of raw implied variance recursively.

        Parameters:
        lambda_param (float): The smoothing parameter lambda.
        tau (int): The number of periods over which the half-life is defined.
        sigma2_SMOOTH_previous (float): The smoothed variance at time t-tau.
        sigma2_RAW_history (list of float): The raw implied variances from time t-tau to t-1.

        Returns:
        float: The smoothed implied variance at time t.
        """
        ewma = lambda_param**tau * sigma2_SMOOTH_previous
        for i in range(tau):
            ewma += (1 - lambda_param) * (lambda_param**i) * sigma2_RAW_history[i]
        return ewma

    def calculate_lambda_with_half_life(self, tau):
        """
        Calculate the smoothing parameter lambda based on the specified half-life tau.

        Parameters:
        tau (float): The half-life of the exponentially-weighted moving average in seconds.

        Returns:
        float: The calculated smoothing parameter lambda.
        """
        lambda_param = np.exp(-np.log(2) / tau)
        return lambda_param

    def calculate_xVIV(self, sigma_smooth_t):
        """
        Calculate the xVIV value based on the given smoothed variance at time t.

        Parameters:
        sigma_smooth_t (float): The smoothed variance at time t.

        Returns:
        float: The calculated xVIV value.
        """
        return 100 * np.sqrt(sigma_smooth_t**2)
