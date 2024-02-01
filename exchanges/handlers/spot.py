from typing import Dict, Any


class SpotMarketHandler:
    def handle(self, symbol: str, market: Dict[str, Any]) -> None:
        print(f"Handling spot market: {symbol}")
        # Implement spot market handling logic here
