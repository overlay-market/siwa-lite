class MarketFilter:
    def __init__(self, exchange, symbol_filter):
        self.exchange = exchange
        self.symbol_filter = symbol_filter

    def filter_markets(self):
        if self.symbol_filter is None:
            return list(self.exchange.markets.values())
        else:
            return [
                market
                for market in self.exchange.markets.values()
                if self.symbol_filter in market["symbol"]
            ]