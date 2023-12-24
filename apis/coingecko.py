from typing import Any, Dict, List
from apis.crypto_api import CryptoAPI
import requests
from apis import utils


class CoinGeckoAPI(CryptoAPI):
    """
    Class to interact with the CoinGecko API.

    Inherits from:
        CryptoAPI: Parent class to provide a common interface for all crypto APIs.

    Methods:
        get_data(N: int) -> Dict[str, Any]:
            Gets data from CoinGecko API.
        extract_market_cap(data: Dict[str, Any]) -> Dict[float, Dict[str, str]]:
            Extracts market cap data from API response.
    """
    VS_CURRENCY = "usd"
    ORDER = "market_cap_desc"
    PAGE = 1
    SPARKLINE = False  # Don't pull last 7 days of data

    NAME_KEY = "name"
    LAST_UPDATED_KEY = "last_updated"
    MARKET_CAP_KEY = "market_cap"

    def __init__(self) -> None:
        """
        Constructs all the necessary attributes for the CoinGeckoAPI object.
        """
        super().__init__(
            url="https://api.coingecko.com/api/v3/coins/markets",
            source='coingecko'
        )

    @utils.handle_request_errors
    def get_data(self, N: int) -> Dict[str, Any]:
        """
        Gets data from CoinGecko API.

        Parameters:
            N (int): Number of cryptocurrencies to fetch.

        Returns:
            Dict[str, Any]: A dictionary with data fetched from API.
        """
        parameters = {
            "vs_currency": self.VS_CURRENCY,
            "order": self.ORDER,
            "per_page": N,
            "page": self.PAGE,
            "sparkline": self.SPARKLINE,
        }
        response = requests.get(self.url, params=parameters)
        data = response.json()
        return data

    def extract_market_cap(self, data: Dict[str, Any]) -> Dict[float, Dict[str, str]]:
        """
        Extracts market cap data from API response.

        Parameters:
            data (Dict[str, Any]): Data received from API.

        Returns:
            Dict[float, Dict[str, str]]:
                A dictionary with market cap as keys and coin details as values.
        """
        market_data = {}
        for coin in data:
            name = coin[self.NAME_KEY]
            last_updated = coin[self.LAST_UPDATED_KEY]
            market_cap = coin[self.MARKET_CAP_KEY]
            market_data[market_cap] = {
                "name": name,
                "last_updated": last_updated,
            }
        return market_data

    @utils.handle_request_errors
    def get_market_caps_of_list(self, tokens: List[str]) -> Dict[str, float]:
        """
        Gets market cap data for the provided list of tokens from CoinGecko API.

        Parameters:
            tokens (List[str]): List of token names for which to fetch market cap data.

        Returns:
            Dict[str, float]: A dictionary with token names as keys and their market cap as values.
        """
        market_caps = {}

        tokens_comma_sep = ','.join(tokens)

        parameters = {
            "vs_currency": self.VS_CURRENCY,
            "ids": tokens_comma_sep,
        }
        response = requests.get(self.url, params=parameters)
        data = response.json()

        if data:
            for d in data:
                market_cap = d.get(self.MARKET_CAP_KEY)
                name = d.get(self.NAME_KEY)
                last_updated = d.get(self.LAST_UPDATED_KEY)
                market_caps[market_cap] = {
                    'name': name,
                    'last_updated': last_updated,
                }
        return market_caps
