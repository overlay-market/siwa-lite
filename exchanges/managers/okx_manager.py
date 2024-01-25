from exchange_manager import ExchangeManager


class OKXManager(ExchangeManager):
    def __init__(self, symbol_filter, market_type):
        super().__init__("okx", symbol_filter, market_type)
