try:
    from apis.utils import get_api_key
except ModuleNotFoundError:
    from utils import get_api_key
from pydantic import BaseModel, ValidationError
import requests
from typing import List
import pandas as pd


# TODO: Add Pydantic models for the data fetched from the API.
class CSGOS2kinsPrice(BaseModel):
    isInflated: bool
    price: int
    count: int
    avg30: int
    createdAt: str


class CSGOS2kinsPrices(BaseModel):
    market_hash_name: str
    prices: List[CSGOS2kinsPrice]


class CSGOS2kins:
    """
    A class to interact with the CSGOSkins API to fetch and process skin
    prices.

    Attributes:
    ----------
    prices_endpoint : str
        The API endpoint for fetching current prices of CSGO skins.
    base_url : str
        The base URL for the CSGOSkins API.
    api_key : str
        The API key to authenticate with the CSGOSkins API.
    """

    API_PREFIX = "PRICE_EMPIRE"
    PRICES_ENDPOINT = "v3/items/prices"
    PRICE_HISTORIES_ENDPOINT = "v3/items/prices/history"
    PRICE_HISTORIES_RPM = 20
    CURRENCY = "USD"
    APP_ID = 730  # Available values : 730, 440, 570, 252490
    SOURCES = "cs2go"
    DEFAULT_BASE_URL = "https://api.pricempire.com/"
    MAPPING_PATH = "apis/csgo/csgo2_mapping.csv"
    DAYS = 7
    CONTENT_TYPE = "application/json"
    DATA_KEY = "data"
    PRICES_KEY = "prices"
    PRICE_KEY = "price"
    QUANTITY_KEY = "quantity"
    QUANTITY_MAP_KEY = "mapped_quantity"
    MARKET_HASH_NAME_KEY = "market_hash_name"
    CONTENT_TYPE_KEY = "Content-Type"

    def __init__(self, base_url=DEFAULT_BASE_URL):
        """
        Initializes the CSGOSkins class with the base URL and API key.

        Parameters:
        ----------
        base_url : str, optional
            The base URL for the CSGOSkins API.
        api_key : str
            The API key to authenticate with the CSGOSkins API.
        """
        self.base_url = base_url
        self.api_key = get_api_key(self.API_PREFIX)
        self.headers = {self.CONTENT_TYPE_KEY: self.CONTENT_TYPE}

    # TODO: Add a method to validate the data fetched from the API using Pydantic.
    # def validate_api_data(self, model: BaseModel, data):
    #     """Validate data pulled from external API using Pydantic."""
    #     try:
    #         for item in data:
    #             model(**item)
    #     except ValidationError as e:
    #         raise Exception(
    #             f"Data pulled from {self.base_url} does not match "
    #             f"pre-defined Pydantic data structure: {e}"
    #         )

    def get_prices(self):
        """
        Retrieves the current prices of CSGO skins from the API.

        Returns:
        -------
        dict
            A dictionary containing the retrieved data from the API.
        """
        url = self.base_url + self.PRICES_ENDPOINT
        payload = {
            "source": self.SOURCES,
            "days": self.DAYS,
            "app_id": self.APP_ID,
            "currency": self.CURRENCY,
            "api_key": self.api_key,
        }
        response = requests.get(url, headers=self.headers, params=payload)
        data = response.json()
        return data

    def get_prices_df(self):
        """
        Fetches the prices of CSGO skins and returns them as a pandas
        DataFrame.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the fetched data from the API.
        """
        data = self.get_prices()

        prices_list = []
        for market_hash_name, details in data.items():
            prices_data = details.get("cs2go", {})
            if prices_data:
                prices_data["market_hash_name"] = market_hash_name
                prices_list.append(prices_data)

        df = pd.DataFrame(prices_list)

        return df


if __name__ == "__main__":
    csgo2 = CSGOS2kins()
    print(csgo2.get_prices_df())
