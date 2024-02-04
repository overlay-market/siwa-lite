import json
import logging
from managers.deribit_manager import DeribitManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        deribit = DeribitManager(
            pairs_to_load="BTC/USD:BTC", market_types=["future", "option"]
        )
        logger.info(f"\nExchange: {deribit.pairs_to_load}")
        deribit.load_specific_pairs()
        result = deribit.get_results()
        with open("Result_option_and_future.json", "w") as f:
            json.dump(result, f, indent=4)

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
