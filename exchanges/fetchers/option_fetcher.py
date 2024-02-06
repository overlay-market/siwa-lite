import logging

import ccxt


class OptionFetcher:
    def __init__(self, exchange: str):
        self.exchange = getattr(ccxt, exchange)()

    def _fetch_data_with_error_handling(self, fetch_function, *args):
        try:
            return fetch_function(*args)
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            logging.error(f"Error fetching data from {self.exchange.name}", e)
            return None

    def fetch_option_order_books(self, symbol):
        response = self.exchange.fetch_order_book(symbol)
        return response

    def fetch_price(self, symbol, price_type):
        return self.exchange.fetch_ticker(symbol)
