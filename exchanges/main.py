import logging

import pandas as pd

from exchanges.managers.binance_manager import BinanceManager
from exchanges.managers.okx_manager import OKXManager
from exchanges.processing import Processing
from managers.deribit_manager import DeribitManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        # deribit = DeribitManager(pairs_to_load=["BTC/USD:BTC"], market_types=["option"])
        # binance = BinanceManager(
        #     pairs_to_load=["BTC/USD:BTC"], market_types=["option", "future"]
        # )
        # okx = OKXManager(pairs_to_load=["BTC/USD:BTC"], market_types=["option"])
        # global_orderbook = pd.DataFrame()
        #
        # for manager in [binance, deribit, okx]:
        #     global_orderbook = pd.concat(
        #         [global_orderbook, manager.load_specific_pairs()], ignore_index=True
        #     )
        # global_orderbook.to_json("global_orderbook.json", orient="records", indent=4)
        global_orderbook = pd.read_json("global_orderbook.json")
        process = Processing()
        x = process.process_global_orderbook(global_orderbook)
        x.to_json("global_orderbook_processed.json", orient="records", indent=4)

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
