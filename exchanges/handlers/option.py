import json
from typing import Dict, Any

from preprocessing import Preprocessing


class OptionMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.preprocessing = Preprocessing(self.exchange, self.market_types)

    def handle(self, symbol: str, market: Dict[str, Any]) -> None:
        """
        Handles a market from an exchange.

        :param symbol: Symbol of the market like BTC/USD
        :param market: Market data like: {
            "symbol": "BTC/USD",
            "price": 10000,
            "timestamp": 123456789,
            "option_type": "call",
            "strike_price": 10000,
            "expiration_date": 123456789,
            "bid_price": 100,
            "ask_price": 200,
            "bid_amount": 1,
            "ask_amount": 1,
            "base_currency": "BTC",
            "quote_currency": "USD",
            "exchange": "binance",
            "market_type": "option"
        }
        """
        with open("option_data.json", "w") as f:
            json.dump(market, f)
        (
            expiry_counts,
            filtered_data,
        ) = self.preprocessing.extract_expiry_and_filter_data(market)

        print("Filtered Data:", filtered_data)
        print("Expiry Counts:", expiry_counts)

    # def process_option_markets(self, option_markets):
    #
    #     # Define constants and parameters for option market processing
    #     index_maturity = 30 / 365  # 30 days in terms of years
    #
    #     markets = self.preprocessing.filter_near_term_options(markets)
    #
    #     (
    #         expiry_counts,
    #         filtered_data,
    #     ) = self.preprocessing.extract_expiry_and_filter_data(option_markets)
    #
    #     most_common_expiry = self.preprocessing.find_most_common_expiry(expiry_counts)
    #
    #     if most_common_expiry:
    #         filtered_data = self.preprocessing.filter_data_by_expiry(
    #             filtered_data, most_common_expiry
    #         )
    #
    #         min_diff_strike = self.preprocessing.find_minimum_difference_strike(
    #             filtered_data
    #         )
    #
    #         if min_diff_strike:
    #             (
    #                 call_data,
    #                 put_data,
    #                 bids,
    #                 asks,
    #             ) = self.preprocessing.extract_call_put_and_bids_asks(
    #                 min_diff_strike, filtered_data
    #             )
    #
    #             implied_forward_price = (
    #                 self.data_filter.calculate_implied_forward_price(
    #                     self, call_data, put_data
    #                 )
    #             )
    #
    #             if implied_forward_price > 0:
    #                 print(f"IF {implied_forward_price}")
    #
    #                 katm_strike = self.preprocessing.calculate_katm_strike(
    #                     call_data, put_data, implied_forward_price
    #                 )
    #
    #                 call_data, put_data = self.preprocessing.select_otm_options(
    #                     call_data,
    #                     put_data,
    #                     katm_strike,
    #                     implied_forward_price,
    #                     RANGE_MULT,
    #                 )
    #
    #                 print("Call Data after OTM selection:", call_data)
    #                 print("Put Data after OTM selection:", put_data)
    #
    #                 near_term_data = [
    #                     option
    #                     for option in filtered_data
    #                     if option["option_type"] == "near_term"
    #                 ]
    #                 next_term_data = [
    #                     option
    #                     for option in filtered_data
    #                     if option["option_type"] == "next_term"
    #                 ]
    #
    #                 implied_variance_near_list = []
    #                 implied_variance_next_list = []
    #
    #                 # Calculate implied variance for near term
    #                 for option_data_near in near_term_data:
    #                     strikes = [
    #                         float(bid[0])
    #                         for bid in option_data_near["order_book"]["bids"]
    #                     ]
    #                     option_prices = [
    #                         float(bid[1])
    #                         for bid in option_data_near["order_book"]["bids"]
    #                     ]
    #
    #                     T_i = option_data_near["time_to_maturity_years"]
    #                     if T_i == 0:
    #                         continue
    #                     r_i = self.data_filter.calculate_implied_interest_rate(
    #                         self, implied_forward_price, katm_strike, T_i
    #                     )
    #                     delta_K = [
    #                         strikes[i + 1] - strikes[i] for i in range(len(strikes) - 1)
    #                     ]
    #
    #                     implied_variance_near = (
    #                         self.preprocessing.calculate_implied_variance(
    #                             implied_forward_price,
    #                             katm_strike,
    #                             strikes,
    #                             option_prices,
    #                             r_i,
    #                             T_i,
    #                             delta_K,
    #                         )
    #                     )
    #
    #                     implied_variance_near_list.append(implied_variance_near)
    #                     print(
    #                         f"Implied Variance (Near Term) for {option_data_near['symbol']}:",
    #                         implied_variance_near,
    #                     )
    #                     # print(f"Interpolated Variance (Near Term) for {option_data['symbol']}:", interpolated_variance)
    #
    #                 # Calculate implied variance for next term
    #                 for option_data_next in next_term_data:
    #                     strikes = [
    #                         float(bid[0])
    #                         for bid in option_data_next["order_book"]["bids"]
    #                     ]
    #                     option_prices = [
    #                         float(bid[1])
    #                         for bid in option_data_next["order_book"]["bids"]
    #                     ]
    #
    #                     T_i = option_data_next["time_to_maturity_years"]
    #                     if T_i == 0:
    #                         continue
    #                     r_i = self.data_filter.calculate_implied_interest_rate(
    #                         self, implied_forward_price, katm_strike, T_i
    #                     )
    #                     delta_K = [
    #                         strikes[i + 1] - strikes[i] for i in range(len(strikes) - 1)
    #                     ]
    #
    #                     next_term_implied_variance = (
    #                         self.preprocessing.calculate_implied_variance(
    #                             implied_forward_price,
    #                             katm_strike,
    #                             strikes,
    #                             option_prices,
    #                             r_i,
    #                             T_i,
    #                             delta_K,
    #                         )
    #                     )
    #
    #                     implied_variance_next_list.append(next_term_implied_variance)
    #                     print(
    #                         f"Implied Variance (Next Term) for {option_data_next['symbol']}:",
    #                         next_term_implied_variance,
    #                     )
    #
    #                 # T_NEAR = [option_data_near["time_to_maturity_years"] for option_data_near in near_term_data]
    #                 # T_NEXT = [option_data_next["time_to_maturity_years"] for option_data_next in next_term_data] * len(T_NEAR)
    #                 # T_INDEX = [index_maturity] * len(T_NEAR)
    #
    #                 # omega_NEAR_t, omega_NEXT_t = self.preprocessing.interpolate_variance(T_NEAR, T_NEXT, T_INDEX)
    #                 # print("Weights for Near Term:", omega_NEAR_t)
    #                 # print("Weights for Next Term:", omega_NEXT_t)
    #
    #                 for implied_variance_near_value in implied_variance_near_list:
    #                     omega_NEAR_t = 0.5
    #                     sigma2_NEAR_t = implied_variance_near_value
    #                     omega_NEXT_t = 0.5  # Replace with actual value
    #                     sigma2_NEXT_t = 0.2  # Replace with actual value
    #
    #                     raw_implied_variance = (
    #                         self.preprocessing.calculate_raw_implied_variance(
    #                             omega_NEAR_t, sigma2_NEAR_t, omega_NEXT_t, sigma2_NEXT_t
    #                         )
    #                     )
    #
    #                     # Now you can use raw_implied_variance as needed
    #                     print("Raw Implied Variance:", raw_implied_variance)
    #
    #                     sigma2_SMOOTH_t_minus_1 = raw_implied_variance
    #
    #                     print(
    #                         "Previous Smoothed Implied Variance:",
    #                         sigma2_SMOOTH_t_minus_1,
    #                     )
    #
    #                     smoothed_implied_variance = self.preprocessing.calculate_ewma(
    #                         lambda_param=0.5,
    #                         sigma2_SMOOTH_t_minus_1=sigma2_SMOOTH_t_minus_1,
    #                         sigma2_RAW_t=sigma2_NEAR_t,
    #                     )
    #                     print("EWMA:", smoothed_implied_variance)
    #
    #                     xVIV_value = self.preprocessing.calculate_xVIV(
    #                         smoothed_implied_variance
    #                     )
    #                     print("xVIV Value:", xVIV_value)
    #
    #         else:
    #             print("No valid bid-ask pairs found in the filtered data.")
    #         # Save the processed and filtered data
    #         self.data_saver.save_data(filtered_data, filename="filtered_data.json")
