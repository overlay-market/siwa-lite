import ccxt
from data_fetcher import DataFetcher
from constants.utils import RANGE_MULT
from market_filter import MarketFilter
from data_saver import DataSaver
from utils import handle_error
from data_filter import DataFilter
import logging
from preprocessing import Preprocessing

# Configure logging to display informational messages
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExchangeManager:
    def __init__(self, exchange_name, symbol_filter, market_type):
        # Initialize the ExchangeManager with exchange name, symbol filter, and market type
        self.exchange = getattr(
            ccxt, exchange_name
        )()  # Initialize exchange using ccxt library
        self.symbol_filter = symbol_filter  # Filter criteria for symbols
        self.exchange_name = exchange_name  # Name of the exchange
        self.market_type = market_type  # Type of market (option or future)
        # Initialize components for data fetching, market filtering, data saving, etc.
        self.data_fetcher = DataFetcher(self.exchange)
        self.market_filter = MarketFilter(self.exchange, self.symbol_filter)
        self.data_saver = DataSaver()
        self.data_filter = DataFilter()
        self.preprocessing = Preprocessing(self.exchange, self.symbol_filter)

    def process_markets(self):
        # Main method to process markets based on the market type
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
        # Process option markets: load markets, filter, and preprocess data
        self.exchange.load_markets()
        markets = self.market_filter.filter_markets()

        # Define constants and parameters for option market processing
        index_maturity = 30 / 365  # 30 days in terms of years

        # markets = self.preprocessing.filter_near_term_options(markets)

        (
            expiry_counts,
            filtered_data,
        ) = self.preprocessing.extract_expiry_and_filter_data(markets)

        most_common_expiry = self.preprocessing.find_most_common_expiry(
            expiry_counts)

        if most_common_expiry:
            filtered_data = self.preprocessing.filter_data_by_expiry(
                filtered_data, most_common_expiry
            )

            min_diff_strike = self.preprocessing.find_minimum_difference_strike(
                filtered_data
            )

            if min_diff_strike:
                (
                    call_data,
                    put_data,
                    bids,
                    asks,
                ) = self.preprocessing.extract_call_put_and_bids_asks(
                    min_diff_strike, filtered_data
                )

                implied_forward_price = self.data_filter.calculate_implied_forward_price(
                    self, call_data, put_data)

                if implied_forward_price > 0:
                    print(f'IF {implied_forward_price}')

                    katm_strike = 3.21
                    call_data, put_data = self.preprocessing.select_otm_options(
                        call_data,
                        put_data,
                        katm_strike,
                        implied_forward_price,
                        RANGE_MULT,
                    )

                    print("Call Data after OTM selection:", call_data)
                    print("Put Data after OTM selection:", put_data)

                    near_term_data = [
                        option for option in filtered_data if option["option_type"] == "near_term"]
                    next_term_data = [
                        option for option in filtered_data if option["option_type"] == "next_term"]

                    implied_variance_near_list = []
                    implied_variance_next_list = []

                    # Calculate implied variance for near term
                    for option_data_near in near_term_data:
                        strikes = [
                            float(bid[0]) for bid in option_data_near["order_book"]["bids"]]
                        option_prices = [
                            float(bid[1]) for bid in option_data_near["order_book"]["bids"]]

                        T_i = option_data_near["time_to_maturity_years"]
                        if T_i == 0:
                            continue
                        r_i = self.data_filter.calculate_implied_interest_rate(
                            self,
                            implied_forward_price,
                            katm_strike,
                            T_i
                        )
                        delta_K = [strikes[i + 1] - strikes[i]
                                   for i in range(len(strikes) - 1)]

                        implied_variance_near = self.preprocessing.calculate_implied_variance(
                            implied_forward_price,
                            katm_strike,
                            strikes,
                            option_prices,
                            r_i,
                            T_i,
                            delta_K,
                        )

                        implied_variance_near_list.append(
                            implied_variance_near)
                        print(
                            f"Implied Variance (Near Term) for {option_data_near['symbol']}:", implied_variance_near)
                        # print(f"Interpolated Variance (Near Term) for {option_data['symbol']}:", interpolated_variance)

                    # Calculate implied variance for next term
                    for option_data_next in next_term_data:
                        strikes = [
                            float(bid[0]) for bid in option_data_next["order_book"]["bids"]]
                        option_prices = [
                            float(bid[1]) for bid in option_data_next["order_book"]["bids"]]

                        T_i = option_data_next["time_to_maturity_years"]
                        if T_i == 0:
                            continue
                        r_i = self.data_filter.calculate_implied_interest_rate(
                            self,
                            implied_forward_price,
                            katm_strike,
                            T_i
                        )
                        delta_K = [strikes[i + 1] - strikes[i]
                                   for i in range(len(strikes) - 1)]

                        next_term_implied_variance = self.preprocessing.calculate_implied_variance(
                            implied_forward_price,
                            katm_strike,
                            strikes,
                            option_prices,
                            r_i,
                            T_i,
                            delta_K,
                        )

                        implied_variance_next_list.append(
                            next_term_implied_variance)
                        print(
                            f"Implied Variance (Next Term) for {option_data_next['symbol']}:", next_term_implied_variance)

                    # T_NEAR = [option_data_near["time_to_maturity_years"] for option_data_near in near_term_data]
                    # T_NEXT = [option_data_next["time_to_maturity_years"] for option_data_next in next_term_data] * len(T_NEAR)
                    # T_INDEX = [index_maturity] * len(T_NEAR)

                    # omega_NEAR_t, omega_NEXT_t = self.preprocessing.interpolate_variance(T_NEAR, T_NEXT, T_INDEX)
                    # print("Weights for Near Term:", omega_NEAR_t)
                    # print("Weights for Next Term:", omega_NEXT_t)

                    for implied_variance_near_value in implied_variance_near_list:
                        omega_NEAR_t = 0.5
                        sigma2_NEAR_t = implied_variance_near_value
                        omega_NEXT_t = 0.5  # Replace with actual value
                        sigma2_NEXT_t = 0.2  # Replace with actual value

                        raw_implied_variance = self.preprocessing.calculate_raw_implied_variance(
                            omega_NEAR_t, sigma2_NEAR_t, omega_NEXT_t, sigma2_NEXT_t
                        )

                        # Now you can use raw_implied_variance as needed
                        print("Raw Implied Variance:", raw_implied_variance)

                        sigma2_SMOOTH_t_minus_1 = raw_implied_variance

                        print("Previous Smoothed Implied Variance:",
                              sigma2_SMOOTH_t_minus_1)

                        smoothed_implied_variance = self.preprocessing.calculate_ewma(
                            lambda_param=0.5,
                            sigma2_SMOOTH_t_minus_1=sigma2_SMOOTH_t_minus_1,
                            sigma2_RAW_t=sigma2_NEAR_t,
                        )
                        print("EWMA:", smoothed_implied_variance)

                        xVIV_value = self.preprocessing.calculate_xVIV(
                            smoothed_implied_variance)
                        print("xVIV Value:", xVIV_value)

            else:
                print("No valid bid-ask pairs found in the filtered data.")
            # Save the processed and filtered data
            self.data_saver.save_data(
                filtered_data, filename="filtered_data.json")

    def process_future_markets(self):
        self.exchange.load_markets()
        # Process future markets: fetch and save future order books
        future_order_books_data = self.data_fetcher.fetch_future_order_books()
        self.data_saver.save_data(future_order_books_data, self.exchange_name)
