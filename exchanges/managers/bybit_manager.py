from exchange_manager import ExchangeManager


class BybitManager(ExchangeManager):
    def __init__(self, symbol_filter, market_type):
        super().__init__("bybit", symbol_filter, market_type)
