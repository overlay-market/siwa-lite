import logging

import pandas as pd
import matplotlib.pyplot as plt

from exchanges.managers.binance_manager import BinanceManager
from exchanges.managers.okx_manager import OKXManager
from exchanges.processing import Processing
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
        okx = OKXManager(pairs_to_load=["BTC/USD:BTC"], market_types=["option"])
        global_orderbook_options = pd.DataFrame()
        global_orderbook_futures = pd.DataFrame()


        for manager in [binance, deribit, okx]:
            global_orderbook_futures = pd.concat(
                [global_orderbook_futures, manager.load_specific_pairs()], ignore_index=True
            )
        global_orderbook_futures.to_json("global_orderbook.json", orient="records", indent=4)
        yield_curve = Processing().calculate_yield_curve(global_orderbook_futures)
        yield_curve.to_json("yield_curve.json", orient="records", indent=4)
        # build graph

        plt.figure(figsize=(10, 6))
        plt.plot(yield_curve['expiry'], yield_curve['implied_interest_rate'], marker='o', linestyle='-',
                 color='blue')
        plt.title('BTC Futures Implied Interest Rate Yield Curve')
        plt.xlabel('Expiry Date')
        plt.ylabel('Average Implied Interest Rate (%)')
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        # global_orderbook = pd.read_json("global_orderbook.json")
        # process = Processing()
        # x = process.process_global_orderbook(global_orderbook)
        # x.to_json("global_orderbook_processed.json", orient="records", indent=4)

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
