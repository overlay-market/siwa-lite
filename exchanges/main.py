import logging
from exchange_manager import ExchangeManager
from managers.binance_manager import BinanceManager
from managers.bybit_manager import BybitManager
from managers.deribit_manager import DeribitManager
from managers.okx_manager import OKXManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        # binance_option = BinanceManager(exchange_name='binance', symbol_filter=None, market_type="option")
        # binance_future = BinanceManager(exchange_name='binance', symbol_filter="BTC", market_type="future")
        deribit_option = DeribitManager(
            symbol_filter="BTC/USD:BTC", market_type="option"
        )
        deribit_future = DeribitManager(
            symbol_filter="BTC/USD:BTC", market_type="future"
        )
        okx_option = OKXManager(symbol_filter="BTC/USD:BTC", market_type="option")
        okx_future = OKXManager(symbol_filter="BTC", market_type="future")
        bybit_option = BybitManager(symbol_filter="BTC/USDC:USDC", market_type="option")
        # bybit_future = BybitManager(symbol_filter="BTC", market_type="future")

        # Process markets for each exchange
        for manager in [
            # binance_option,
            # binance_future,
            # deribit_option,
            # deribit_future,
            okx_option,
            # okx_future,
            # bybit_option,
            # bybit_future,
        ]:
            logger.info(f"\nExchange: {manager.exchange_name}")
            manager.process_markets()

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
