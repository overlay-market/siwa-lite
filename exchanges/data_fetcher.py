from utils import handle_error


class DataFetcher:
    def __init__(self, exchange):
        self.exchange = exchange

    def fetch_data(self, symbol):
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            order_book = self.exchange.fetch_order_book(symbol)
            return {
                "symbol": symbol,
                "last_price": ticker["last"],
                "order_book": order_book,
            }
        except Exception as e:
            handle_error(f"Error fetching data for {symbol}", e)
            return None
