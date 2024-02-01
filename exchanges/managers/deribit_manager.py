from exchange_manager import ExchangeManager


class DeribitManager(ExchangeManager):
    def __init__(self, pairs_to_load, market_types):
        super().__init__("deribit", pairs_to_load, market_types)
