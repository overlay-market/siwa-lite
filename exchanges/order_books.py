import ccxt
import requests
import logging
from typing import Optional, List, Dict, Union
import time
import schedule
import os
import math
from datetime import datetime
import json


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SPREAD_MULTIPLIER = 10
SPREAD_MIN = 0.0005

BINANCE_API_URL = 'https://eapi.binance.com/eapi/v1/exchangeInfo'


class ExchangeManager:
    def __init__(self, exchange_name: str, symbol_filter: Optional[str], market_type: str):
        self.exchange = getattr(ccxt, exchange_name)()
        self.symbol_filter = symbol_filter
        self.exchange_name = exchange_name
        self.market_type = market_type
        self.data: Dict[str,
                        Dict[str, Union[List[float], List[float], float]]] = {}

    def _handle_error(self, error_message: str, exception: Exception):
        logger.error(f"{error_message}: {exception}")

    def initialize_exchange(self):
        try:
            self.exchange.load_markets()
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error initializing exchange '{self.exchange_name}'", e)

    def filter_markets(self) -> List[Dict[str, Union[str, int, float]]]:
        try:
            if self.symbol_filter is None:
                return list(self.exchange.markets.values())
            else:
                filtered_markets = [market for market in self.exchange.markets.values()
                                    if self.symbol_filter in market['symbol']]
                return [market for market in filtered_markets if market['symbol'].endswith(('-C', '-P'))]
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error filtering markets for exchange '{self.exchange_name}'", e)
            return []

    def calculate_implied_interest_rate(self, forward_price: float, spot_price: float, time_to_maturity_years: float) -> float:
        try:
            return (math.log(forward_price) - math.log(spot_price)) / time_to_maturity_years if time_to_maturity_years else 0
        except ZeroDivisionError:
            return 0

    def calculate_yield_curve(self, option_data_list: List[Dict[str, Union[str, int, float]]]) -> Dict[str, List[float]]:
        yield_curve = {}
        try:
            for option_data in option_data_list:
                symbol = option_data.get('symbol')
                forward_price = option_data.get('mark_price')
                spot_price = option_data.get('current_spot_price')
                time_to_maturity_years = option_data.get(
                    'time_to_maturity_years')

                if symbol and forward_price is not None and spot_price is not None and time_to_maturity_years is not None:
                    implied_interest_rate = self.calculate_implied_interest_rate(
                        forward_price, spot_price, time_to_maturity_years)
                    if symbol not in yield_curve:
                        yield_curve[symbol] = []
                    yield_curve[symbol].append(implied_interest_rate)
                    print(implied_interest_rate)

            return yield_curve
        except Exception as e:
            self._handle_error("Error calculating yield curve", e)
            return {}

    def calculate_time_to_maturity(self, option_order_books_data):
        try:
            symbol = option_order_books_data.get('symbol')

            if symbol is None:
                return 0

            expiration_date_str = symbol.split('-')[1]
            date_format = '%y%m%d' if len(
                expiration_date_str) == 6 else '%d%m%Y'

            current_date = datetime.utcnow()
            current_date = datetime.strptime(current_date.strftime(
                "%Y-%m-%d %H:%M:%S.%f"), "%Y-%m-%d %H:%M:%S.%f")
            expiration_date = datetime.strptime(
                expiration_date_str, date_format)

            time_to_maturity_seconds = (
                expiration_date - current_date).total_seconds()
            time_to_maturity_days = time_to_maturity_seconds / (24 * 3600)
            time_to_maturity_years = time_to_maturity_days / 365.0

            return max(time_to_maturity_years, 0)

        except Exception as e:
            self._handle_error("Error calculating time to maturity", e)
            return 0

    def calculate_implied_forward_price(self, call_data: dict, put_data: dict) -> float:
        try:
            call_price = call_data.get('mid_price')
            put_price = put_data.get('mid_price')
            strike_price = float(call_data.get(
                'order_book', {}).get('bids', [])[0][0])

            if call_price is None or put_price is None:
                return 0  # Handle the case where either call or put prices are not available

            forward_price = strike_price + \
                strike_price * (call_price - put_price)

            return forward_price
        except Exception as e:
            self._handle_error("Error calculating implied forward price", e)
            return 0

    def _fetch_price(self, symbol: str, price_type: str):
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            price = ticker.get(price_type)
            return price
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error fetching {price_type} for symbol '{symbol}'", e)
            return None

    def _fetch_mark_price(self, symbol: str):
        try:
            mark_price = self._fetch_price(
                symbol, 'markPrice') or self._fetch_price(symbol, 'mark_price')

            if mark_price is not None and mark_price != float('inf'):
                return mark_price

            bid, ask = self._fetch_price(
                symbol, 'bid'), self._fetch_price(symbol, 'ask')
            return (bid + ask) / 2 if bid is not None and ask is not None else None

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error fetching spot price for symbol '{symbol}'", e)
            return None

    def _standardize_data(self, symbol: str, order_book_data: dict) -> dict:
        try:
            spot_price = self._fetch_price(symbol, 'last')
            mark_price = self._fetch_mark_price(symbol)
            time_to_maturity = self.calculate_time_to_maturity(order_book_data)

            if spot_price is None or mark_price is None:
                return {}

            bids, asks = order_book_data.get(
                'bids', []), order_book_data.get('asks', [])

            if not bids or not asks:
                return {}

            max_bid = max(float(bid[0]) for bid in bids)
            min_ask = min(float(ask[0]) for ask in asks)

            mark_price_info = order_book_data.get('info', {}).get(
                'markPrice') or order_book_data.get('info', {}).get('mark_price')
            mark_price = mark_price_info if mark_price_info is not None and mark_price_info != float(
                'inf') else self._fetch_mark_price(symbol)

            if mark_price is None:
                mark_price = (order_book_data['bid'] + order_book_data['ask']) / \
                    2 if 'bid' in order_book_data and 'ask' in order_book_data else None

            selected_option = min(asks, key=lambda x: float(x[0]) - max_bid)
            selected_mark_price = float(selected_option[0])
            mid_price = (max_bid + min_ask) / 2

            standardized_data = {
                'symbol': symbol,
                'order_book': order_book_data,
                'current_spot_price': spot_price,
                'mark_price': selected_mark_price,
                'mid_price': mid_price,
                'time_to_maturity_years': time_to_maturity,
            }

            if not self.is_valid_quote(standardized_data):
                return {}

            return standardized_data

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error standardizing data for symbol '{symbol}'", e)
            return {}

    def is_valid_quote(self, data: dict) -> bool:
        try:
            mark_price = float(data.get('mark_price', 0))
            bid_price = float(data.get('order_book', {}).get('bids', [])[0][0])
            ask_price = float(data.get('order_book', {}).get('asks', [])[0][0])

            bid_spread, ask_spread = max(
                0, mark_price - bid_price), max(0, ask_price - mark_price)

            # Calculate MAS (Maximum Allowed Spread)
            # mas = min(bid_spread, ask_spread) * SPREAD_MULTIPLIER

            # # Calculate GMS (Global Maximum Spread)
            # gms = SPREAD_MIN * SPREAD_MULTIPLIER

            # spread = bid_spread + ask_spread

            # # Remove the quote if its spread is greater than both GMS and MAS
            # if spread > mas and spread > gms:
            #     return False

            if bid_price > ask_price or not (bid_price <= mark_price <= ask_price) or mark_price <= 0:
                return False

            if bid_spread < 0 or ask_spread < 0 or mark_price <= 0:
                return False

            return True
        except (ValueError, IndexError):
            return False

    def fetch_option_order_books(self, symbol: str, limit: int = 100):
        try:
            response = self.exchange.fetch_order_book(symbol, limit=limit)
            standardized_data = self._standardize_data(symbol, response)
            return standardized_data
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error(
                f"Error fetching order book for symbol '{symbol}'", e)

    def fetch_binance_option_symbols(self):
        try:
            response = requests.get(BINANCE_API_URL)

            if response.status_code == 200:
                exchange_info = response.json()

                for symbol_info in exchange_info.get("optionSymbols", []):
                    if 'BTC' in symbol_info.get("symbol"):
                        symbol = symbol_info.get("symbol")
                        print(symbol)
                        data = self.fetch_option_order_books(symbol)
                        time.sleep(5)
                        self.save_data_to_file('binance', data)

            else:
                logger.error(f"Error: {response.status_code}")
                return []
        except (requests.RequestException, Exception) as e:
            self._handle_error("Error fetching Binance option symbols", e)
            return []

    def fetch_future_order_books(self, limit: int = 100):
        try:
            markets = {symbol: market for symbol, market in self.exchange.markets.items()
                       if self.symbol_filter in symbol}
            future_markets = {symbol: market for symbol,
                              market in markets.items() if market.get('future', True)}

            for symbol, market in future_markets.items():
                response = self.exchange.fetch_order_book(symbol, limit=limit)
                standardized_data = self._standardize_data(symbol, response)
                self.save_data_to_file(self.exchange_name, standardized_data)
                print(standardized_data)

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            self._handle_error("Error fetching future order books", e)

    def save_data_to_file(self, exchange_name: str, data: dict, filename: str = 'data.txt'):
        try:
            folder_name = "data_folders"
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)

            exchange_filename = os.path.join(
                folder_name, f"{exchange_name}_{filename}")

            if not data:
                logging.warning(
                    f"Skipping save: Empty data for {exchange_name}")
                return

            existing_data = []
            if os.path.exists(exchange_filename):
                with open(exchange_filename, 'r') as file:
                    try:
                        existing_data = json.load(file)
                    except json.JSONDecodeError as e:
                        logging.warning(
                            f"Error decoding JSON from {exchange_filename}: {e}")

            existing_data.append(data)

            with open(exchange_filename, 'w') as file:
                json.dump(existing_data, file, indent=2)

            logging.info(
                f"Data for {exchange_name} saved to {exchange_filename}")
        except Exception as e:
            self._handle_error("Error saving data to file", e)

    def heartbeat(self):
        try:
            logger.info("Heartbeat: Polling prices every 30 seconds...")

            with open(f"data_folders/{self.exchange_name}_data.txt", 'r') as file:
                data_list = json.load(file)

            for item in data_list:
                symbol = item.get('symbol')
                if symbol is not None:
                    mark_price = self._fetch_mark_price(symbol)
                    if mark_price is not None:
                        print(mark_price)
                        item['mark_price'] = mark_price

            with open(f"data_folders/{self.exchange_name}_data.txt", 'w') as file:
                json.dump(data_list, file, indent=2)

            logger.info(
                f"Heartbeat: Prices updated and saved to {self.exchange_name}_data.txt")

        except Exception as e:
            self._handle_error("Error in heartbeat", e)

    def schedule_heartbeat(self):
        schedule.every(30).seconds.do(self.heartbeat)

        while True:
            schedule.run_pending()
            time.sleep(1)

    def process_yield_curve(self):
        try:
            with open(f"data_folders/{self.exchange_name}_data.txt", 'r') as file:
                data_list = json.load(file)

            yield_curve = self.calculate_yield_curve(data_list)

            average_yield_curve = {}
            for symbol, interest_rates in yield_curve.items():
                average_interest_rate = sum(
                    interest_rates) / len(interest_rates)
                average_yield_curve[symbol] = average_interest_rate

            logger.info(
                f"Yield Curve for {self.exchange_name}:\n{average_yield_curve}")

        except Exception as e:
            self._handle_error("Error processing yield curve", e)

    def process_markets(self):
        try:
            self.initialize_exchange()

            if self.market_type == 'option':
                markets = self.filter_markets()

                # Assuming index maturity is set at 30 days
                index_maturity = 30 / 365  # Index maturity in years

                for market in markets:
                    symbol = market['symbol']
                    option_order_books_data = self.fetch_option_order_books(
                        symbol)
                    self.save_data_to_file(
                        self.exchange_name, option_order_books_data)

                    # # Calculate time to maturity for the option
                    # time_to_maturity = option_order_books_data.get('time_to_maturity_years', 0)

                    # if time_to_maturity <= index_maturity:
                    #     # Near-term option
                    #     self.save_data_to_file(self.exchange_name, option_order_books_data)

                    # elif time_to_maturity > index_maturity:
                    #     # Next-term option
                    #     self.save_data_to_file(self.exchange_name, option_order_books_data)

            elif self.market_type == 'future':
                future_order_books_data = self.fetch_future_order_books()
                self.save_data_to_file(
                    self.exchange_name, future_order_books_data)

            else:
                logging.error("Invalid market type: %s", self.market_type)

        except (ccxt.NetworkError, ccxt.ExchangeError, Exception) as e:
            self._handle_error(
                f"Error processing markets for exchange '{self.exchange_name}'", e)


def main():
    exchanges = [
        # ExchangeManager(exchange_name='binance',
        #                 symbol_filter=None, market_type='option'),
        # ExchangeManager(exchange_name='binance',
        #                 symbol_filter='BTC', market_type='future'),
        # ExchangeManager(exchange_name='deribit',
        #                 symbol_filter='BTC', market_type='option'),
        ExchangeManager(exchange_name='deribit',
                        symbol_filter='BTC/USD:BTC', market_type='future'),
        # ExchangeManager(exchange_name='okx',
        #                 symbol_filter='BTC', market_type='option'),
        # ExchangeManager(exchange_name='okx',
        #                 symbol_filter='BTC', market_type='future'),
        # ExchangeManager(exchange_name='bybit',
        #                 symbol_filter='BTC', market_type='option'),
        # ExchangeManager(exchange_name='bybit',
        #                 symbol_filter='BTC/USDC:USDC', market_type='future'),
    ]

    try:
        for exchange in exchanges:
            logger.info(f"\nExchange: {exchange.exchange_name}")
            exchange.process_markets()
            # exchange.process_yield_curve()
            # if hasattr(exchange, 'schedule_heartbeat'):
            #     exchange.schedule_heartbeat()

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
