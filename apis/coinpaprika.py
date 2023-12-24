from typing import Any, Dict, List
from apis.crypto_api import CryptoAPI
import requests
from apis import utils


class CoinPaprikaAPI(CryptoAPI):
    """
    Class to interact with the CoinPaprika API.

    Inherits from:
        CryptoAPI: Parent class to provide a common interface for all crypto APIs.

    Attributes:
        ohlc_url (str): OHLC (Open/High/Low/Close) data URL format of the API.

    Methods:
        get_data(N: int) -> List[Dict[str, Any]]:
            Gets data from CoinPaprika API.
        extract_market_cap(data: List[Dict[str, Any]]) -> Dict[float, Dict[str, Any]]:
            Extracts market cap data from API response.
    """

    def __init__(self) -> None:
        """
        Constructs all the necessary attributes for the CoinPaprikaAPI object.
        """
        self.ohlc_url = "https://api.coinpaprika.com/v1/coins/{coin_id}/ohlcv/latest"  # noqa E501
        super().__init__(
            url="https://api.coinpaprika.com/v1/coins",
            source='coinpaprika'
        )

    @utils.handle_request_errors
    def get_data(self, N: int) -> List[Dict[str, Any]]:
        """
        Gets data from CoinPaprika API.

        Parameters:
            N (int): Number of cryptocurrencies to fetch.

        Returns:
            List[Dict[str, Any]]:
                A list of dictionaries with data fetched from API.
        """
        response = requests.get(self.url)
        # HTTP 200 status code means the request was successful
        if response.status_code == 200:
            data = response.json()
        else:
            raise requests.exceptions.RequestException(
                f"Received status code {response.status_code} "
                f"for URL: {self.url}"
            )
        # Sorting the coins by market cap
        # Also filtering out coins with rank 0 (junk values in API response)
        filtered_data = [coin for coin in data if coin['rank'] != 0]
        sorted_data = sorted(filtered_data, key=lambda coin: coin['rank'])[:N]
        return sorted_data

    @utils.handle_request_errors
    def extract_market_cap(self, data: List[Dict[str, Any]]) -> Dict[float, Dict[str, Any]]:
        """
        Extracts market cap data from API response.

        Parameters:
            data (List[Dict[str, Any]]): Data received from API.

        Returns:
            Dict[float, Dict[str, Any]]:
                A dictionary with market cap as keys and coin details as values.
        """
        # Fetch the details of each coin
        market_data = {}
        for coin in data:
            coin_id = coin["id"]
            coin_info_url = self.ohlc_url.format(coin_id=coin_id)
            coin_info_response = requests.get(coin_info_url)
            # HTTP 200 status code means the request was successful
            if coin_info_response.status_code == 200:
                coin_info = coin_info_response.json()
            else:
                raise requests.exceptions.RequestException(
                    f"Received status code {coin_info_response.status_code} "
                    "for URL: {coin_info_url}"
                )

            name = coin["name"]
            last_updated = 0  # Updated every 5 mins as per docs: https://api.coinpaprika.com/#tag/Coins/paths/~1coins~1%7Bcoin_id%7D~1ohlcv~1today~1/get # noqa E501
            market_cap = coin_info[0]['market_cap']
            market_data[market_cap] = {
                "name": name,
                "last_updated": last_updated,
            }

        return market_data
