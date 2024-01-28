import ccxt
from data_fetcher import DataFetcher
from exchanges.constants.utils import RANGE_MULT
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
            handle_error(f"Error processing markets for {self.exchange_name}", e)

    def process_option_markets(self):
        # Process option markets: load markets, filter, and preprocess data
        self.exchange.load_markets()
        markets = self.market_filter.filter_markets()

        # Define constants and parameters for option market processing
        index_maturity = 30 / 365  # 30 days in terms of years

        markets = self.preprocessing.filter_near_term_options(markets)

        (
            expiry_counts,
            filtered_data,
        ) = self.preprocessing.extract_expiry_and_filter_data(markets)

        most_common_expiry = self.preprocessing.find_most_common_expiry(expiry_counts)

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

                implied_forward_price = 0.23

                if implied_forward_price > 0:
                    self.preprocessing.calculate_implied_forward_price(
                        call_data, bids, implied_forward_price
                    )

                    katm_strike = float(call_data["order_book"]["bids"][0][0])
                    call_data, put_data = self.preprocessing.select_otm_options(
                        call_data,
                        put_data,
                        katm_strike,
                        implied_forward_price,
                        RANGE_MULT,
                    )

                    print("Call Data after OTM selection:", call_data)
                    print("Put Data after OTM selection:", put_data)

            else:
                print("No valid bid-ask pairs found in the filtered data.")
            # Save the processed and filtered data
            self.data_saver.save_data(filtered_data, filename="filtered_data.json")

    def process_future_markets(self):
        # Process future markets: fetch and save future order books
        future_order_books_data = self.data_fetcher.fetch_future_order_books()
        self.data_saver.save_data(future_order_books_data, self.exchange_name)
