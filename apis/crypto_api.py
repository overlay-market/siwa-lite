from typing import Any, List, Dict
from apis import utils
import os


class CryptoAPI:
    """
    Class to interact with a Crypto API (ex: coingecko, coinmarketcap, etc.)

    Attributes:
        url (str): URL of the API.
        source (str): Source of the data.

    Methods:
        fetch_mcap_by_rank(N: int) -> dict:
            Fetch data by market capitalization and stores in a database.
        get_data(N: int):
            Abstract method to get data.
        extract_market_cap(data: Any):
            Abstract method to extract market cap data.
    """

    API_KEYS_FILE = 'api_keys.json'

    def __init__(self, url: str, source: str) -> None:
        """
        Constructs all the necessary attributes for the CryptoAPI object.

        Parameters:
            url (str): URL of the API.
            source (str): Source of the data.
        """
        self.url = url
        self.source = source

    def fetch_mcap_by_list(self, tokens: List[str]) -> Dict[str, float]:
        """
        Fetch data by list of tokens, store it in a database and return
        the data.

        Parameters:
            tokens (List[str]): List of tokens to fetch.

        Returns:
            Dict[str, float]:
                Dictionary with token names as keys and market cap as values.
        """

        market_data = self.get_market_caps_of_list(tokens)
        if market_data is None:
            return None

        # Store market data in the database
        utils.create_market_cap_database()
        utils.store_market_cap_data(
            market_data=market_data, source=self.source
        )
        return market_data

    def fetch_mcap_by_rank(self, N: int) -> dict:
        """
        Fetch data by market capitalization, store it in a database and return
        the data.

        Parameters:
            N (int): Number of cryptocurrencies to fetch.

        Returns:
            dict:
                Dictionary with market cap as keys and other details as values.
        """
        data = self.get_data(N)
        if data is None:
            return None
        else:
            market_data = self.extract_market_cap(data)

        # Store market data in the database
        utils.create_market_cap_database()
        utils.store_market_cap_data(
            market_data=market_data, source=self.source
        )
        return market_data

    def get_data(self, N: int) -> Any:
        """
        Abstract method to get data from API.

        Parameters:
            N (int): Number of cryptocurrencies to fetch.

        Raises:
            NotImplementedError:
                If this method is not implemented by a subclass.
        """
        raise NotImplementedError

    def extract_market_cap(self, data: Any) -> dict:
        """
        Abstract method to extract market cap data from API response.

        Parameters:
            data (Any): Data received from API.

        Raises:
            NotImplementedError:
                If this method is not implemented by a subclass.
        """
        raise NotImplementedError

    def get_api_key(self, api_provider_name: str) -> str:
        """
        Retrieves the API key for the specified API provider.

        Parameters:
            api_provider_name (str): Name of the API provider.

        Returns:
            str: API key for the API provider.

        Raises:
            Exception: If the API key for the provider is not found in the
            environment variables.
        """
        api_key = os.getenv(f"{api_provider_name.upper()}_API_KEY")
        if not api_key:
            raise Exception(
                f"No API key found for {api_provider_name}. "
                "Please set it in your environment variables as "
                f"{api_provider_name.upper()}_API_KEY."
            )
        return api_key
