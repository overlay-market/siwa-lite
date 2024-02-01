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
        self.market_filter = MarketFilter(
            self.exchange_name, self.symbol_filter)
        self.data_saver = DataSaver()
        self.data_filter = DataFilter()

    def select_otm_options(self, call_data, put_data, katm_strike, implied_forward_price, range_mult):
        # Calculate Kmin and Kmax
        k_min = implied_forward_price / range_mult
        k_max = implied_forward_price * range_mult

        call_strike = float(call_data["order_book"]["bids"][0][0])
        put_strike = float(put_data["order_book"]["asks"][0][0])

        # Select OTM options
        if not (k_min < call_strike < k_max):
            call_data["mid_price"] = None
        if not (k_min < put_strike < k_max):
            put_data["mid_price"] = None

        # If both call and put options are selected for the same strike (KATM), take the average mid-price
        if call_strike == put_strike == katm_strike:
            avg_mid_price = (call_data.get("mid_price", 0) +
                             put_data.get("mid_price", 0)) / 2
            call_data["mid_price"] = put_data["mid_price"] = avg_mid_price

        return call_data, put_data

    def extract_expiry_and_filter_data(self, markets):
        expiry_counts = Counter()
        filtered_data = []

        for market in markets[:100]:
            symbol = market.get("symbol")
            option_order_books_data = self.data_fetcher.fetch_option_order_books(
                symbol)

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

            self.data_saver.save_data(
                option_order_books_data, self.exchange_name)

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
            filtered_data, key=lambda x: x.get(
                "order_book", {}).get("bids", [])[0][0]
        )

        grouped_data = {}
        for key, group in groupby(
            sorted_data, key=lambda x: x.get("symbol").split("-")[1]
        ):
            grouped_data[key] = list(group)

        filtered_data_after_threshold = []

        # Set the minimum bid threshold based on tick size
        tick_size = 0.0005

        for expiry_date, data_list in grouped_data.items():
            sorted_data_by_strike = sorted(
                data_list, key=lambda x: x.get(
                    "order_book", {}).get("bids", [])[0][0]
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

                diff_value = abs(
                    float(diff_strike[0][0]) - float(diff_strike[1][0]))

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

    def extract_time_to_maturity_near_term(self, filtered_data):
        near_term_values = [data['time_to_maturity_years']
                            for data in filtered_data if data['option_type'] == 'near_term']
        # Return a list, even if empty
        return near_term_values if near_term_values else [0.0]

    def extract_time_to_maturity_next_term(self, filtered_data):
        next_term_values = [data['time_to_maturity_years']
                            for data in filtered_data if data['option_type'] == 'next_term']
        return next_term_values if next_term_values else [0.0]

    def calculate_implied_forward_price(self, call_data, put_data, bids, implied_forward_price):
        largest_strike = 0
        min_mid_price_diff = float('inf')  # Initialize with a large value

        for bid in bids:
            if len(bid) >= 2:
                bid_strike = float(bid[0])
                call_price = float(bid[1])

                # Assuming put_data is available with corresponding ask prices
                # Adjust as per your actual data structure
                put_price = float(put_data["order_book"]["asks"][0][1])

                mid_price_diff = abs(call_price - put_price)

                if bid_strike < implied_forward_price and mid_price_diff < min_mid_price_diff:
                    largest_strike = bid_strike
                    min_mid_price_diff = mid_price_diff

        if largest_strike > 0:
            print(
                f"Largest Strike (KATM) for {call_data['order_book']['bids'][0][0]}: {largest_strike}"
            )

            # Calculate implied forward price using the formula: Fimp = K + F × (C − P )
            call_price = float(call_data["order_book"]["bids"][0][1])
            # Adjust as per your actual data structure
            put_price = float(put_data["order_book"]["asks"][0][1])
            forward_price = implied_forward_price

            implied_forward_price = largest_strike + \
                forward_price * (call_price - put_price)

            print(f"Implied Forward Price: {implied_forward_price}")

        else:
            print("No valid bid-ask pairs found in the filtered data.")

    def calculate_implied_variance(
        self, F_i, K_i_ATM, strikes, option_prices, r_i, T_i, delta_K
    ):
        # Precompute constant
        discount_factor = np.exp(r_i * T_i)

        # Explicitly convert delta_K elements to float
        delta_K = np.array(delta_K, dtype=float)

        # Ensure that all arrays have the same length
        min_length = min(len(strikes), len(option_prices), len(delta_K))
        strikes = np.array(strikes[:min_length], dtype=float)
        option_prices = np.array(option_prices[:min_length], dtype=float)

        # Log-linear extrapolation
        Kmin, Kmax = min(strikes), max(strikes)
        extrapolated_strikes = np.logspace(
            np.log10(Kmin), np.log10(Kmax), num=1000)
        extrapolated_option_prices = np.interp(
            extrapolated_strikes, strikes, option_prices)

        # Log-linear piece-wise interpolation
        interpolated_strikes = np.logspace(
            np.log10(Kmin), np.log10(Kmax), num=1000)
        interpolated_option_prices = np.interp(
            interpolated_strikes, strikes, option_prices)

        # Update strikes and option prices
        strikes = np.concatenate(
            [strikes, extrapolated_strikes, interpolated_strikes])
        option_prices = np.concatenate(
            [option_prices, extrapolated_option_prices, interpolated_option_prices])

        # Reshape arrays to have the same shape for broadcasting
        discount_factor = discount_factor.reshape((1,))
        strikes = strikes.reshape((len(strikes), 1))
        option_prices = option_prices.reshape((len(option_prices), 1))

        # Vectorized operations with broadcasting
        weights = discount_factor * (delta_K / strikes**2)
        sum_term = np.sum(weights * option_prices)

        # Calculate the implied variance using the formula
        implied_variance = (1 / T_i) * (2 * sum_term -
                                        ((F_i / K_i_ATM) - 1) ** 2)

        return implied_variance

    def interpolate_variance(self, T_NEAR, T_NEXT, T_INDEX):
        """
        Calculate the weights for the near and next term variances based on the given times to maturity.

        Parameters:
        T_NEAR (list): List containing Time to maturity for the near term.
        T_NEXT (list): List containing Time to maturity for the next term.
        T_INDEX (list): List containing Time to maturity for the index.

        Returns:
        tuple: A tuple containing the weights for the near term (omega_NEAR) and the next term (omega_NEXT).
        """
        if len(T_NEAR) != len(T_NEXT) or len(T_NEXT) != len(T_INDEX):
            raise ValueError("Input lists must have the same length")

        omega_NEAR_t = [(T_NEXT[i] - T_INDEX[i]) / (T_NEXT[i] - T_NEAR[i] +
                                                    1e-9) / (T_INDEX[i] + 1e-9) for i in range(len(T_NEAR))]
        omega_NEXT_t = [(T_INDEX[i] - T_NEAR[i]) / (T_NEXT[i] - T_NEAR[i] +
                                                    1e-9) / (T_NEXT[i] + 1e-9) for i in range(len(T_NEAR))]

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
            lambda_param * sigma2_SMOOTH_t_minus_1 +
            (1 - lambda_param) * sigma2_RAW_t
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
            ewma += (1 - lambda_param) * (lambda_param**i) * \
                sigma2_RAW_history[i]
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
