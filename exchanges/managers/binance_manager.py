from exchanges.exchange_manager import ExchangeManager


class BinanceManager(ExchangeManager):
    def __init__(self, symbol_filter, market_type):
        super().__init__("binance", symbol_filter, market_type)
