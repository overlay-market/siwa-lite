import json
from typing import Dict, Any

from preprocessing import Preprocessing
from data_fetcher import DataFetcher
from consolidate_data import ConsolidateData
import ccxt
import logging
import requests
from constants.urls import BINANCE_API_URL
import time
from utils import handle_error
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptionMarketHandler:
    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = DataFetcher(self.exchange)
        self.preprocessing = Preprocessing(self.exchange, self.market_types)
        self.consolidate_data = ConsolidateData(self.exchange)

    def handle(self, symbol: str, market) -> None:
        order_books = []
        raw_dara = []
        for slice in market:
            order = self.data_fetcher.fetch_option_order_books(slice)
            raw_dara.append(order)
            if order is None or not order:
                continue
            df = pd.DataFrame({
                'symbol': [order['symbol']],
                'bid_price': [order['order_book']['bids'][0][0]],
                'ask_price': [order['order_book']['asks'][0][0]],
                # 'mark_price': mark_price,
                'timestamp': [order['order_book']['timestamp']],
                'datetime': [order['order_book']['datetime']],
                # 'underlying_price': [order['current_spot_price']['underlying_price']],
                'time_to_maturity_years': [order['time_to_maturity_years']],
                'mid_price': [order['mid_price']],
                'mark_price': [order['mark_price']],

            })

            # Convert timestamp to readable format (if needed)
            df['datetime_readable'] = pd.to_datetime(df['timestamp'], unit='ms').astype(str)

            # Convert DataFrame to dictionary
            order_book_dict = df.to_dict(orient='records')

            order_books.append(order_book_dict)

    def _fetch_prices(self, symbol):
        # Fetch and return the spot price and the mark price for the symbol.
        spot_price = self.data_fetcher.fetch_price(symbol, "last")
        mark_price = self.data_fetcher.fetch_mark_price(symbol)
        return spot_price, mark_price

    # def _fetch_data_with_error_handling(self, fetch_function, *args):
    #     try:
    #         return fetch_function(*args)
    #     except (ccxt.NetworkError, ccxt.ExchangeError) as e:
    #         return None
    #
    # def fetch_option_order_books(self, symbol):
    #     response = self.exchange.fetch_order_book(symbol)
    #     st = self.consolidate_data.standardize_data(symbol, response)
    #     return st
    #
    # def fetch_binance_option_symbols(self):
    #     try:
    #         response = requests.get(BINANCE_API_URL)
    #         if response.status_code != 200:
    #             logger.error(f"Error: {response.status_code}")
    #             return []
    #
    #         exchange_info = response.json()
    #         for symbol_info in exchange_info.get("optionSymbols", []):
    #             if "BTC" in symbol_info.get("symbol"):
    #                 symbol = symbol_info.get("symbol")
    #                 data = self.fetch_option_order_books(symbol)
    #                 if data:
    #                     self.save_data_to_file("binance", data)
    #                     time.sleep(5)
    #
    #         return []
    #
    #     except requests.RequestException as e:
    #         handle_error("Error fetching Binance option symbols", e)
    #         return []
    #
    # def fetch_future_order_books(self, limit=100):
    #     future_markets = self._filter_future_markets()
    #     for symbol in future_markets:
    #         standardized_data = self.fetch_option_order_books(symbol, limit)
    #         if standardized_data:
    #             self.save_data_to_file(self.exchange.name, standardized_data)
    #
    # def _filter_future_markets(self):
    #     return [
    #         symbol
    #         for symbol, market in self.exchange.markets.items()
    #         if market.get("future", False)
    #     ]
    #
    # def fetch_price(self, symbol, price_type):
    #     return self.exchange.fetch_ticker(symbol)
    #
    # def fetch_mark_price(self, symbol):
    #     mark_price = self.fetch_price(symbol, "markPrice") or self.fetch_price(
    #         symbol, "mark_price"
    #     )
    #     if mark_price is not None and mark_price != float("inf"):
    #         return mark_price
    #
    #     bid, ask = self.fetch_price(symbol, "bid"), self.fetch_price(symbol, "ask")
    #     return (bid + ask) / 2 if bid and ask else None

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
