import unittest
from unittest.mock import MagicMock

from exchanges.data_fetcher import DataFetcher


class TestDataFetcher(unittest.TestCase):
    def setUp(self):
        # Create a mock exchange object for testing
        self.mock_exchange = MagicMock()

        # Create a DataFetcher instance using the mock exchange
        self.data_fetcher = DataFetcher(self.mock_exchange)

    def test_fetch_option_order_books(self):
        # Define a mock response for fetch_order_book
        mock_response = {
            "bids": [["10", "20"], ["11", "22"]],
            "asks": [["12", "24"], ["13", "26"]],
        }

        # Set the return value of fetch_order_book to the mock response
        self.mock_exchange.fetch_order_book.return_value = mock_response

        # Call the fetch_option_order_books method
        standardized_data = self.data_fetcher.fetch_option_order_books("BTC/USDT")

        # Assert that the method returns the expected standardized data
        expected_data = {
            "symbol": "BTC/USDT",
            "bids": [{"price": 10, "quantity": 20}, {"price": 11, "quantity": 22}],
            "asks": [{"price": 12, "quantity": 24}, {"price": 13, "quantity": 26}],
        }
        self.assertEqual(standardized_data, expected_data)

    def test_fetch_price(self):
        # Define a mock ticker response
        mock_ticker = {
            "symbol": "BTC/USDT",
            "ask": "40000",
            "bid": "39900",
            "markPrice": "39950",
        }

        # Set the return value of fetch_ticker to the mock ticker response
        self.mock_exchange.fetch_ticker.return_value = mock_ticker

        # Test fetching the ask price
        ask_price = self.data_fetcher.fetch_price("BTC/USDT", "ask")
        self.assertEqual(ask_price, 40000)

        # Test fetching the bid price
        bid_price = self.data_fetcher.fetch_price("BTC/USDT", "bid")
        self.assertEqual(bid_price, 39900)

        # Test fetching the mark price
        mark_price = self.data_fetcher.fetch_price("BTC/USDT", "markPrice")
        self.assertEqual(mark_price, 39950)

    def test_fetch_mark_price(self):
        # Define a mock ticker response with markPrice
        mock_ticker_with_mark_price = {
            "symbol": "BTC/USDT",
            "markPrice": "39950",
        }

        # Set the return value of fetch_ticker to the mock ticker response
        self.mock_exchange.fetch_ticker.return_value = mock_ticker_with_mark_price

        # Test fetching markPrice
        mark_price = self.data_fetcher.fetch_mark_price("BTC/USDT")
        self.assertEqual(mark_price, 39950)

        # Define a mock ticker response without markPrice
        mock_ticker_without_mark_price = {
            "symbol": "BTC/USDT",
            "ask": "40000",
            "bid": "39900",
        }

        # Set the return value of fetch_ticker to the mock ticker response
        self.mock_exchange.fetch_ticker.return_value = mock_ticker_without_mark_price

        # Test fetching markPrice when it's not available
        mark_price = self.data_fetcher.fetch_mark_price("BTC/USDT")
        expected_calculated_price = (40000 + 39900) / 2
        self.assertEqual(mark_price, expected_calculated_price)


if __name__ == "__main__":
    unittest.main()
