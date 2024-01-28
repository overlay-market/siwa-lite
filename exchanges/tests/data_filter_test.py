import unittest

from exchanges.data_filter import DataFilter


class TestDataFilter(unittest.TestCase):
    def test_calculate_implied_interest_rate(self):
        # Test for normal conditions
        self.assertAlmostEqual(
            DataFilter.calculate_implied_interest_rate(None, 105, 100, 0.5),
            0.0976,
            places=4,
        )
        # Test for zero time to maturity
        self.assertEqual(
            DataFilter.calculate_implied_interest_rate(None, 105, 100, 0), 0
        )
        # Test for negative forward price
        self.assertAlmostEqual(
            DataFilter.calculate_implied_interest_rate(None, -105, 100, 0.5),
            -0.2075,
            places=4,
        )

    def test_calculate_implied_forward_price(self):
        # Test for normal conditions
        call_data = {"mid_price": 5, "order_book": {"bids": [[100, 1]]}}
        put_data = {"mid_price": 3}
        self.assertEqual(
            DataFilter.calculate_implied_forward_price(None, call_data, put_data), 102
        )
        # Test for missing call or put prices
        self.assertEqual(
            DataFilter.calculate_implied_forward_price(None, {}, put_data), 0
        )

    def test_calculate_yield_curve(self):
        # Test for normal conditions
        option_data_list = [
            {
                "symbol": "BTC-220625",
                "mark_price": 105,
                "current_spot_price": 100,
                "time_to_maturity_years": 0.5,
            },
            {
                "symbol": "BTC-220625",
                "mark_price": 110,
                "current_spot_price": 100,
                "time_to_maturity_years": 0.5,
            },
        ]
        expected_yield_curve = {"BTC-220625": [0.0976, 0.1904]}
        result = DataFilter.calculate_yield_curve(None, option_data_list)
        for symbol in expected_yield_curve:
            self.assertAlmostEqual(
                result[symbol][0], expected_yield_curve[symbol][0], places=4
            )
            self.assertAlmostEqual(
                result[symbol][1], expected_yield_curve[symbol][1], places=4
            )

    def test_calculate_time_to_maturity(self):
        # Test for normal conditions
        option_order_books_data = {
            "symbol": "BTC-220625"
        }  # Set a specific future date for testing
        result = DataFilter.calculate_time_to_maturity(None, option_order_books_data)
        self.assertTrue(result > 0)
        # Test for missing symbol
        self.assertEqual(DataFilter.calculate_time_to_maturity(None, {}), 0)


if __name__ == "__main__":
    unittest.main()
