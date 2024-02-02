from exchange_manager import ExchangeManager


class BybitManager(ExchangeManager):
    def __init__(self, pairs_to_load, market_types):
        super().__init__("bybit", pairs_to_load, market_types)
