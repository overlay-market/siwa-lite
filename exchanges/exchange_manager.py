import ccxt
from data_fetcher import DataFetcher
from market_filter import MarketFilter
from data_saver import DataSaver
from utils import handle_error
from data_filter import DataFilter
from collections import Counter
from datetime import datetime


class ExchangeManager:
    def __init__(self, exchange_name: str, symbol_filter: str, market_type: str):
        self.exchange = getattr(ccxt, exchange_name)()
        self.symbol_filter = symbol_filter
        self.exchange_name = exchange_name
        self.market_type = market_type
        self.data_fetcher = DataFetcher(self.exchange)
        self.market_filter = MarketFilter(self.exchange, self.symbol_filter)
        self.data_saver = DataSaver()
        # self.data_filter = DataFilter()

    def process_markets(self):
        try:
            if self.market_type == "option":
                self.exchange.load_markets()
                filtered_markets = self.market_filter.filter_markets()

                index_maturity = 30 / 365

                markets = [
                    market
                    for market in filtered_markets
                    if market.get("option_type") != "near_term"
                ]
                expiry_counts = Counter()

                filtered_data = []
                for market in filtered_markets:
                    symbol = market["symbol"]
                    option_order_books_data = (
                        self.data_fetcher.fetch_option_order_books(symbol)
                    )
                    if not option_order_books_data:
                        print(f"No data fetched for symbol: {symbol}")
                        continue

                    expiration_date_str = symbol.split("-")[1]
                    date_format = "%y%m%d"
                    expiration_date = datetime.strptime(
                        expiration_date_str, date_format
                    )

                    current_date = datetime.utcnow()
                    time_to_maturity_days = (expiration_date - current_date).days
                    time_to_maturity_years = time_to_maturity_days / 365.0

                    print(time_to_maturity_years)

                    if time_to_maturity_years <= index_maturity:
                        # Near-term option
                        option_order_books_data["option_type"] = "near_term"
                    elif time_to_maturity_years > index_maturity:
                        # Next-term option
                        option_order_books_data["option_type"] = "next_term"

                    # Save the data after setting the option_type
                    self.data_saver.save_data(
                        option_order_books_data, self.exchange_name
                    )

                    # Count the occurrences of each expiry date
                    expiry_counts[expiration_date] += 1

                    # Add the data to the filtered_data list
                    filtered_data.append(option_order_books_data)

                # Find the expiry date with the highest count
                most_common_expiry = expiry_counts.most_common(1)
                if most_common_expiry:
                    most_common_expiry = most_common_expiry[0][0]

                    filtered_data = [
                        data
                        for data in filtered_data
                        if datetime.strptime(
                            data.get("symbol").split("-")[1], date_format
                        )
                        == most_common_expiry
                    ]

                    min_diff_strike = None
                    min_diff_value = float("inf")

                    for data in filtered_data:
                        bids = data.get("order_book", {}).get("bids", [])
                        asks = data.get("order_book", {}).get("asks", [])
                        if bids and asks:
                            diff_strike = min(
                                zip(bids, asks),
                                key=lambda x: abs(float(x[0][0]) - float(x[1][0])),
                            )

                            diff_value = abs(
                                float(diff_strike[0][0]) - float(diff_strike[1][0])
                            )

                            if diff_value < min_diff_value:
                                min_diff_value = diff_value
                                min_diff_strike = diff_strike
                                print(min_diff_value)

                    if min_diff_strike:
                        call_data = {
                            "mid_price": min_diff_strike[0][0],
                            "order_book": {"bids": [min_diff_strike[0]]},
                        }
                        put_data = {
                            "mid_price": min_diff_strike[1][0],
                            "order_book": {"asks": [min_diff_strike[1]]},
                        }

                        if "bids" in option_order_books_data.get(
                            "order_book", {}
                        ) and "asks" in option_order_books_data.get("order_book", {}):
                            bids, asks = (
                                option_order_books_data["order_book"]["bids"],
                                option_order_books_data["order_book"]["asks"],
                            )

                        implied_forward_price = self.calculate_implied_forward_price(
                            call_data, put_data
                        )

                        # Calculate implied forward price for the strike with minimum difference
                        if implied_forward_price > 0:
                            print(
                                f"Implied Forward Price for {symbol}: {implied_forward_price}"
                            )

                            # Find the largest strike less than the implied forward price
                            largest_strike = 0
                            for bid in bids:
                                if (
                                    len(bid) >= 2
                                ):  # Ensure bid has at least two elements (price and quantity)
                                    bid_strike = float(bid[0])
                                    print(
                                        f"Checking bid_strike: {bid_strike}, implied_forward_price: {implied_forward_price}, largest_strike: {largest_strike}"
                                    )
                                    if (
                                        bid_strike < implied_forward_price
                                        and bid_strike > largest_strike
                                    ):
                                        largest_strike = bid_strike
                                        print(
                                            f"Updating largest_strike: {largest_strike}"
                                        )

                            if largest_strike > 0:
                                print(
                                    f"Largest Strike (KATM) for {symbol}: {largest_strike}"
                                )
                            else:
                                print(
                                    "No valid bid-ask pairs found in the filtered data."
                                )

                    else:
                        print("No valid bid-ask pairs found in the filtered data.")

                    # Save the filtered data to a file
                    self.data_saver.save_data(
                        filtered_data,
                        filename="filtered_data.json",
                        exchange_name=self.exchange_name,
                    )

        except Exception as e:
            handle_error(f"Error processing markets for {self.exchange_name}", e)
