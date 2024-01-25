from exchanges.exchange_manager import ExchangeManager


class DeribitManager(ExchangeManager):
    def __init__(self, symbol_filter, market_type):
        super().__init__("deribit", symbol_filter, market_type)
