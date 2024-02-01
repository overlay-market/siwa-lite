from typing import Dict, Any

from preprocessing import Preprocessing


class OptionMarketHandler:
    def handle(self, symbol: str, market: Dict[str, Any]) -> None:
        with open("option_market.txt", "a") as f:
            f.write(f"{symbol}: {market}\n")

        self.preprocessing = Preprocessing()
        markets = self.preprocessing.filter_near_term_options(self.markets)
        print(f"markets: {markets}")
        (
            expiry_counts,
            filtered_data,
        ) = self.preprocessing.extract_expiry_and_filter_data(markets)

        print(f"expiry_counts: {expiry_counts}")
        print(f"filtered_data: {filtered_data}")
