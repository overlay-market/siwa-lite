import pandas as pd
import json


class MergeMarketHandler:
    def handle(self, options_market, future_market):
        # Merge the data from the three markets
        print("It was successfully")
        merge_market = pd.concat([options_market, future_market], ignore_index=True)
        print(f"Merged market: {merge_market}")
        with open("merge_market.json", "w") as f:
            json.dump(merge_market, f)
