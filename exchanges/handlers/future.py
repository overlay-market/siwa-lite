from typing import Dict, Any
import json
from data_fetcher import DataFetcher
import pandas as pd
from utils import handle_error
import math

class FutureMarketHandler:

    def __init__(self, exchange, market_types):
        self.exchange = exchange
        self.market_types = market_types
        self.data_fetcher = DataFetcher(exchange)


    def handle(self, symbol: str, market: Dict[str, Any]) -> None:

        self.fetch_future_order_books(market)

    def fetch_future_order_books(self, market):
        order_filtered = []
        for order in market:
            standardized_data = self.data_fetcher.fetch_option_order_books(order)
            df = pd.DataFrame({
                'symbol': [standardized_data['symbol']],
                'bid_price': [standardized_data['order_book']['bids'][0][0]],
                'ask_price': [standardized_data['order_book']['asks'][0][0]],
                'timestamp': [standardized_data['order_book']['timestamp']],
                'datetime': [standardized_data['order_book']['datetime']],
                # 'time_to_maturity_years': [order['time_to_maturity_years']],
                'mid_price': [standardized_data['mid_price']],
                'mark_price': [standardized_data['mark_price']],

            })


            # # Convert timestamp to readable format (if needed)
            # df['time_to_maturity_years'] = pd.to_datetime(df['timestamp'], unit='ms').astype(str)

            # Convert DataFrame to dictionary
            yield_curve = self.calculate_yield_curve(df.to_dict(orient='records'))
            print(yield_curve)
            df['yield_curve'] = [yield_curve]
            order_book_dict = df.to_dict(orient='records')


            order_filtered.append(order_book_dict)
        with open("future_order_books.json", "w") as f:
            json.dump(order_filtered, f, indent=4)

    def calculate_implied_interest_rate(
            self, forward_price, spot_price, time_to_maturity_years
    ):
        try:
            return (
                (math.log(forward_price) - math.log(spot_price))
                / time_to_maturity_years
                if time_to_maturity_years
                else 0
            )
        except ZeroDivisionError:
            return 0
    # TODO: Finish this func
    def calculate_yield_curve(self, option_data_list):
        yield_curve = {}
        try:
            for option_data in option_data_list:
                symbol = option_data.get("symbol")
                forward_price = option_data.get("mark_price")
                spot_price = 3.4
                time_to_maturity_years = 0.0011184415228944697

                if (
                    symbol
                    and forward_price is not None
                    and spot_price is not None
                    and time_to_maturity_years is not None
                ):
                    implied_interest_rate = self.calculate_implied_interest_rate(
                        forward_price, spot_price, time_to_maturity_years
                    )

                    if symbol not in yield_curve:
                        yield_curve[symbol] = []
                    yield_curve[symbol].append(implied_interest_rate)
                    print(implied_interest_rate)

            return yield_curve
        except Exception as e:
            handle_error("Error calculating yield curve", e)
            return {}

