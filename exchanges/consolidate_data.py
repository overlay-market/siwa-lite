import json

import ccxt
from data_filter import DataFilter
from order_books import SPREAD_MIN, SPREAD_MULTIPLIER
from utils import handle_error


class ConsolidateData:
    def __init__(self, exchange):
        # Initialize with an exchange object and create a DataFilter instance.
        self.exchange = exchange
        self.data_filter = DataFilter()
        self.data_fetcher = None  # Initialize a placeholder for the DataFetcher.

    def standardize_data(self, symbol, order_book_data):
        try:
            from data_fetcher import DataFetcher

            self.data_fetcher = DataFetcher(
                self.exchange
            )  # Initialize the DataFetcher for the exchange.

            # Fetch the spot price and mark price for the given symbol.
            spot_price, mark_price = self._fetch_prices(symbol)
            with open('spot_price.json', 'w') as f:
                json.dump(spot_price, f)
            print(f'Spot {spot_price}')
            print(f'Mark {mark_price}')

            # Return empty if prices are not available.
            if not spot_price or not mark_price:
                return {}

            # Extract bids and asks from the order book data.
            bids, asks = order_book_data.get("bids", []), order_book_data.get(
                "asks", []
            )
            if not bids or not asks:
                return {}

            # Determine the maximum bid price and minimum ask price.
            max_bid, min_ask = self._get_max_min_prices(bids, asks)

            # Find the price of the selected option based on minimum ask spread.
            selected_option_price = min(asks, key=lambda x: float(x[0]) - max_bid)[0]
            mid_price = (max_bid + min_ask) / 2  # Calculate the mid-price.

            # Prepare the standardized data structure.
            standardized_data = {
                "symbol": symbol,
                "order_book": order_book_data,
                "current_spot_price": spot_price,
                "mark_price": float(selected_option_price),
                "mid_price": mid_price,
                # "time_to_maturity_years": 2.3
            }

            # Check if the quote is valid and return the data, else return empty.
            return standardized_data if self._is_valid_quote(standardized_data) else {}

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            # Handle any network or exchange-related errors.
            handle_error(f"Error standardizing data for symbol '{symbol}'", e)
            return {}

    def _fetch_prices(self, symbol):
        # Fetch and return the spot price and the mark price for the symbol.
        spot_price = self.data_fetcher.fetch_price(symbol, "last")
        mark_price = self.data_fetcher.fetch_mark_price(symbol)
        return spot_price, mark_price

    def _get_max_min_prices(self, bids, asks):
        # Calculate the maximum bid and minimum ask prices from order book data.
        max_bid = max(float(bid[0]) for bid in bids)
        min_ask = min(float(ask[0]) for ask in asks)
        return max_bid, min_ask

    def _is_valid_quote(self, data):
        # Validate the quote based on bid, ask, and mark prices and their spreads.
        bid_price, ask_price = (
            data["order_book"]["bids"][0][0],
            data["order_book"]["asks"][0][0],
        )
        mark_price = data["mark_price"]

        # Calculate bid and ask spreads.
        bid_spread, ask_spread = max(0, mark_price - bid_price), max(
            0, ask_price - mark_price
        )
        mas = min(bid_spread, ask_spread) * SPREAD_MULTIPLIER  # Maximum Allowed Spread.
        gms = SPREAD_MIN * SPREAD_MULTIPLIER  # Global Maximum Spread.
        spread = bid_spread + ask_spread

        # Check if the spread is within limits and the price ordering is correct.
        return (
            spread <= max(mas, gms)
            and bid_price <= mark_price <= ask_price
            and mark_price > 0
        )
