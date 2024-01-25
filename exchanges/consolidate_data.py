from constants.utils import SPREAD_MULTIPLIER, SPREAD_MIN
import ccxt
from data_filter import DataFilter


class ConsolidateData:

    def __init__(self, exchange):
        self.exchange = exchange
        self.data_filter = DataFilter()

    def standardize_data(self, symbol, order_book_data):
        try:
            from data_fetcher import DataFetcher

            self.data_fetcher = DataFetcher(self.exchange)
            spot_price = self.data_fetcher.fetch_price(symbol, "last")
            mark_price = self.data_fetcher.fetch_mark_price(symbol)
            time_to_maturity = self.data_filter.calculate_time_to_maturity(
                self, order_book_data)

            if spot_price is None or mark_price is None:
                return {}

            bids, asks = order_book_data.get("bids", []), order_book_data.get(
                "asks", []
            )

            if not bids or not asks:
                return {}

            max_bid = max(float(bid[0]) for bid in bids)
            min_ask = min(float(ask[0]) for ask in asks)

            mark_price_info = order_book_data.get("info", {}).get(
                "markPrice"
            ) or order_book_data.get("info", {}).get("mark_price")
            mark_price = (
                mark_price_info
                if mark_price_info is not None and mark_price_info != float("inf")
                else self.data_fetcher.fetch_mark_price(symbol)
            )

            if mark_price is None:
                mark_price = (
                    (order_book_data["bid"] + order_book_data["ask"]) / 2
                    if "bid" in order_book_data and "ask" in order_book_data
                    else None
                )

            selected_option = min(asks, key=lambda x: float(x[0]) - max_bid)
            selected_mark_price = float(selected_option[0])
            mid_price = (max_bid + min_ask) / 2

            standardized_data = {
                "symbol": symbol,
                "order_book": order_book_data,
                "current_spot_price": spot_price,
                "mark_price": selected_mark_price,
                "mid_price": mid_price,
                "time_to_maturity_years": time_to_maturity,
            }

            if not self._is_valid_quote(standardized_data):
                return {}

            return standardized_data

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self.logging(
                f"Error standardizing data for symbol '{symbol}'", e)
            return {}

    def _is_valid_quote(self, data):
        try:
            mark_price = float(data.get("mark_price", 0))
            bid_price = float(data.get("order_book", {}).get("bids", [])[0][0])
            ask_price = float(data.get("order_book", {}).get("asks", [])[0][0])

            bid_spread, ask_spread = max(0, mark_price - bid_price), max(
                0, ask_price - mark_price
            )

            # Calculate MAS (Maximum Allowed Spread)
            mas = min(bid_spread, ask_spread) * SPREAD_MULTIPLIER

            # Calculate GMS (Global Maximum Spread)
            gms = SPREAD_MIN * SPREAD_MULTIPLIER

            spread = bid_spread + ask_spread

            # Remove the quote if its spread is greater than both GMS and MAS
            if spread > mas and spread > gms:
                return False

            if (
                bid_price > ask_price
                or not (bid_price <= mark_price <= ask_price)
                or mark_price <= 0
            ):
                return False

            if bid_spread < 0 or ask_spread < 0 or mark_price <= 0:
                return False

            return True
        except (ValueError, IndexError):
            return False
