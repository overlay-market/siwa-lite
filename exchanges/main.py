import logging

import pandas as pd
import matplotlib.pyplot as plt

from exchanges.managers.binance_manager import BinanceManager
from exchanges.managers.okx_manager import OKXManager
from exchanges.processing import Processing
from managers.deribit_manager import DeribitManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        # deribit = DeribitManager(pairs_to_load=["BTC/USD:BTC"], market_types=["option"])
        # binance = BinanceManager(
        #     pairs_to_load=["BTC/USD:BTC"], market_types=["option", "future"]
        # )
        # okx = OKXManager(pairs_to_load=["BTC/USD:BTC"], market_types=["option"])
        # global_orderbook_options = pd.DataFrame()
        # global_orderbook_futures = pd.DataFrame()
        #
        #
        # for manager in [binance, deribit]:
        #     options, futures = manager.load_specific_pairs()
        #     global_orderbook_options = pd.concat([global_orderbook_options, options]).reset_index(drop=True)
        #     global_orderbook_futures = pd.concat([global_orderbook_futures, futures]).reset_index(drop=True)
        #
        # consolidated_options = Processing().consolidate_quotes(global_orderbook_options)
        #
        # global_orderbook_futures.to_json("futures.json", orient="records", indent=4)
        #
        # process_quotes = Processing().process_quotes(consolidated_options)
        # process_quotes.to_json("quotes.json", orient="records", indent=4)
        # process_quotes.to_csv("quotes.csv", index=False)
        process_quotes = pd.read_json("quotes.json")
        futures = pd.read_json("futures.json")
        print(futures.to_string())
        filter_near_next_term_options = Processing().filter_near_next_term_options(process_quotes)
        near_term_options, next_term_options = filter_near_next_term_options
        near_term_options.to_json("near_term_options.json", orient="records", indent=4)
        next_term_options.to_json("next_term_options.json", orient="records", indent=4)
        # calculate_implied_forward_price = Processing().calculate_implied_forward_price(process_quotes)
        # filtered_options = Processing().filter_and_sort_options(process_quotes, calculate_implied_forward_price)
        # filtered_options.to_json("filtered_options.json", orient="records", indent=4)
        near_term_implied_forward_price = Processing().calculate_implied_forward_price(near_term_options)
        next_term_implied_forward_price = Processing().calculate_implied_forward_price(next_term_options)
        near_term_filtered_options = Processing().filter_and_sort_options(near_term_options, near_term_implied_forward_price)
        next_term_filtered_options = Processing().filter_and_sort_options(next_term_options, next_term_implied_forward_price)
        near_term_filtered_options.to_csv("near_term_filtered_options.csv", index=False)
        next_term_filtered_options.to_csv("next_term_filtered_options.csv", index=False)
        find_missing_expiries_near_term = Processing().find_missing_expiries(near_term_filtered_options, futures)
        interpolate_implied_interest_rates_near_term = Processing().interpolate_implied_interest_rates(near_term_filtered_options, find_missing_expiries_near_term)
        interpolate_implied_interest_rates_near_term.to_csv("interpolate_implied_interest_rates_near_term.csv", index=False)
        interpolate_implied_interest_rates_near_term.to_json("interpolate_implied_interest_rates_near_term.json", orient="records", indent=4)



    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
