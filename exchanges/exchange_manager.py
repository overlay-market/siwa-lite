import ccxt
from data_fetcher import DataFetcher
from market_filter import MarketFilter
from data_saver import DataSaver
from utils import handle_error


class ExchangeManager:
    def __init__(self, exchange_name: str, symbol_filter: str, market_type: str):
        self.exchange = getattr(ccxt, exchange_name)()
        self.symbol_filter = symbol_filter
        self.exchange_name = exchange_name
        self.market_type = market_type
        self.data_fetcher = DataFetcher(self.exchange)
        self.market_filter = MarketFilter(self.exchange, self.symbol_filter)
        self.data_saver = DataSaver()

    def process_markets(self):
        try:
            self.exchange.load_markets()
            filtered_markets = self.market_filter.filter_markets()
            for market in filtered_markets:
                symbol = market["symbol"]
                data = self.data_fetcher.fetch_data(symbol)
                self.data_saver.save_data(data, f"{self.exchange_name}_data.txt")
        except Exception as e:
            handle_error(f"Error processing markets for {self.exchange_name}", e)
