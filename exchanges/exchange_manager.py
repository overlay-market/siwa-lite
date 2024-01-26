import ccxt
from data_fetcher import DataFetcher
from market_filter import MarketFilter
from data_saver import DataSaver
from utils import handle_error
from data_filter import DataFilter
from itertools import groupby
from collections import Counter
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExchangeManager:
    def __init__(self, exchange_name, symbol_filter, market_type):
        self.exchange = getattr(ccxt, exchange_name)()
        self.symbol_filter = symbol_filter
        self.exchange_name = exchange_name
        self.market_type = market_type
        self.data_fetcher = DataFetcher(self.exchange)
        self.market_filter = MarketFilter(self.exchange, self.symbol_filter)
        self.data_saver = DataSaver()
        self.data_filter = DataFilter()

    def process_markets(self):
        try:
            if self.market_type == "option":
                self.process_option_markets()
            elif self.market_type == "future":
                self.process_future_markets()
            else:
                logging.error("Invalid market type: %s", self.market_type)

        except Exception as e:
            handle_error(
                f"Error processing markets for {self.exchange_name}", e)

    def process_option_markets(self):
        self.exchange.load_markets()
        markets = self.market_filter.filter_markets()

        index_maturity = 30 / 365  # 30 days in terms of years

        markets = self.filter_near_term_options(markets)

        expiry_counts, filtered_data = self.extract_expiry_and_filter_data(
            markets)

        most_common_expiry = self.find_most_common_expiry(expiry_counts)

        if most_common_expiry:
            filtered_data = self.filter_data_by_expiry(
                filtered_data, most_common_expiry
            )

            min_diff_strike = self.find_minimum_difference_strike(
                filtered_data)

            if min_diff_strike:
                call_data, put_data, bids, asks = self.extract_call_put_and_bids_asks(
                    min_diff_strike, filtered_data
                )

                implied_forward_price = 0.23

                if implied_forward_price > 0:
                    self.calculate_implied_forward_price(
                        call_data, bids, implied_forward_price
                    )

                    rm = 2.5
                    katm_strike = float(call_data["order_book"]["bids"][0][0])
                    call_data, put_data = self.select_otm_options(
                        call_data, put_data, katm_strike, implied_forward_price, rm)

                    print("Call Data after OTM selection:", call_data)
                    print("Put Data after OTM selection:", put_data)

            else:
                print("No valid bid-ask pairs found in the filtered data.")

            self.data_saver.save_data(
                filtered_data, filename="filtered_data.json")

    def select_otm_options(self, call_data, put_data, katm_strike, implied_forward_price, range_mult=2.5):
        # Calculate Kmin and Kmax
        k_min = implied_forward_price / range_mult
        k_max = implied_forward_price * range_mult

        call_strike = float(call_data["order_book"]["bids"][0][0])
        put_strike = float(put_data["order_book"]["asks"][0][0])

        if k_min < call_strike < k_max and k_min < put_strike < k_max:
            # If both call and put options are within the specified range, keep them
            return call_data, put_data
        else:
            # Otherwise, skip the option that is outside the range
            if call_strike < k_min or call_strike > k_max:
                call_data["mid_price"] = None  # Skip the call option
            if put_strike < k_min or put_strike > k_max:
                put_data["mid_price"] = None  # Skip the put option

        return call_data, put_data

    def filter_near_term_options(self, markets):
        return [
            market for market in markets if market.get("option_type") != "near_term"
        ]

    def extract_expiry_and_filter_data(self, markets):
        expiry_counts = Counter()
        filtered_data = []

        for market in markets:
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

            if time_to_maturity_years <= 30 / 365:
                option_order_books_data["option_type"] = "near_term"
            elif time_to_maturity_years > 30 / 365:
                option_order_books_data["option_type"] = "next_term"

            self.data_saver.save_data(
                option_order_books_data, self.exchange_name)

            expiry_counts[expiration_date] += 1
            filtered_data.append(option_order_books_data)

        return expiry_counts, filtered_data

    def extract_expiration_date(self, symbol):
        expiration_date_str = symbol.split("-")[1]
        date_format = "%y%m%d"
        return datetime.strptime(expiration_date_str, date_format)

    def find_most_common_expiry(self, expiry_counts):
        most_common_expiry = expiry_counts.most_common(1)
        return most_common_expiry[0][0] if most_common_expiry else None

    def filter_data_by_expiry(self, filtered_data, most_common_expiry):
        date_format = "%y%m%d"
        sorted_data = sorted(filtered_data, key=lambda x: x.get(
            "order_book", {}).get("bids", [])[0][0])

        grouped_data = {}
        for key, group in groupby(sorted_data, key=lambda x: x.get("symbol").split("-")[1]):
            grouped_data[key] = list(group)

        filtered_data_after_threshold = []

        # Set the minimum bid threshold based on tick size
        tick_size = 0.01  # Replace with the actual tick size for your options

        for expiry_date, data_list in grouped_data.items():
            sorted_data_by_strike = sorted(data_list, key=lambda x: x.get(
                "order_book", {}).get("bids", [])[0][0])
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

    def calculate_implied_forward_price(self, call_data, bids, implied_forward_price):
        largest_strike = 0
        for bid in bids:
            if len(bid) >= 2:
                bid_strike = float(bid[0])
                if bid_strike < implied_forward_price and bid_strike > largest_strike:
                    largest_strike = bid_strike

        if largest_strike > 0:
            print(
                f"Largest Strike (KATM) for {call_data['order_book']['bids'][0][0]}: {largest_strike}"
            )
        else:
            print("No valid bid-ask pairs found in the filtered data.")

    def process_future_markets(self):
        future_order_books_data = self.data_fetcher.fetch_future_order_books()
        self.data_saver.save_data(future_order_books_data, self.exchange_name)
