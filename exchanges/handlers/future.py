from typing import Dict, Any
import json
from data_fetcher import DataFetcher
import pandas as pd

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
            print(df)

            # # Convert timestamp to readable format (if needed)
            # df['time_to_maturity_years'] = pd.to_datetime(df['timestamp'], unit='ms').astype(str)

            # Convert DataFrame to dictionary
            order_book_dict = df.to_dict(orient='records')
            print(order_book_dict)

            order_filtered.append(order_book_dict)
        with open("future_order_books.json", "w") as f:
            json.dump(order_filtered, f, indent=4)
