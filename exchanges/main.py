import logging

import pandas as pd

from exchanges.managers.binance_manager import BinanceManager
from exchanges.managers.okx_manager import OKXManager
from managers.deribit_manager import DeribitManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        deribit = DeribitManager(pairs_to_load=["BTC/USD:BTC"], market_types=["option"])
        binance = BinanceManager(
            pairs_to_load=["BTC/USD:BTC"], market_types=["option", "future"]
        )
        okx = OKXManager(
            pairs_to_load=["BTC/USD:BTC"], market_types=["option", "future"]
        )
        results = pd.DataFrame()

        for manager in [deribit]:
            results = pd.concat(
                [results, manager.load_specific_pairs()], ignore_index=True
            )

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
