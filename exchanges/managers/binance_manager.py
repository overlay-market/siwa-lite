from exchange_manager import ExchangeManager


class BinanceManager(ExchangeManager):
    def __init__(self, pairs_to_load, market_types):
        super().__init__("binance", pairs_to_load, market_types)
