from exchanges.exchange_manager import ExchangeManager


class OKXManager(ExchangeManager):
    def __init__(self, pairs_to_load, market_types):
        super().__init__("okx", pairs_to_load, market_types)
