try:
    from apis.utils import get_api_key
except ModuleNotFoundError:
    from utils import get_api_key
from pydantic import BaseModel, ValidationError, conint, conlist, confloat, confloat
import requests
from typing import Dict, Optional, List
import pandas as pd


class Source(BaseModel):
    isInflated: Optional[bool] = None
    price: Optional[int] = None
    count: Optional[int] = None
    avg30: Optional[int] = None
    createdAt: Optional[str] = None


class Skin(BaseModel):
    liquidity: Optional[float] = None
    sources: Dict[str, Source] = {}

    class Config:
        extra = "allow"


class CSGOS2kinsPrices(BaseModel):
    prices: Dict[str, Skin]


class CSGOS2kinsHistory(BaseModel):
    history: Dict[str, List[int]]


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
    CURRENCY = "USD"
    APP_ID = 730  # Available values : 730, 440, 570, 252490
    SOURCES = "cs2go"
    DEFAULT_BASE_URL = "https://api.pricempire.com/"
    DAYS = 7
    CONTENT_TYPE = "application/json"
    CONTENT_TYPE_KEY = "Content-Type"

    def __init__(self, base_url=DEFAULT_BASE_URL):
        """
        Initializes the CSGOSkins class with the base URL and API key.

        Parameters:
        ----------
        base_url : str, optional
            The base URL for the CSGOSkins API.
        """
        self.base_url = base_url
        self.api_key = get_api_key(self.API_PREFIX)
        self.headers = {self.CONTENT_TYPE_KEY: self.CONTENT_TYPE}

    def validate_api_data(self, model: BaseModel, data):
        """
        Validate data pulled from external API using Pydantic.

        Parameters:
        -----------
        model : pydantic.BaseModel
            The Pydantic model to validate against.
        data : dict
            The data pulled from the API.

        Raises:
        -------
        Exception
            If the data does not match the pre-defined Pydantic data structure.

        """
        print("Data from API:", data)  # Print out the data before validation
        try:
            for market_hash_name, item in data.items():
                model(prices={market_hash_name: item})
        except ValidationError as e:
            raise Exception(
                f"Data pulled from {self.base_url} does not match "
                f"pre-defined Pydantic data structure: {e}"
            )

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
        self.validate_api_data(CSGOS2kinsPrices, data)
        return data

    def get_prices_df(self):
        """
        Fetches the prices of CSGO skins and returns them as a pandas
        DataFrame.

        Returns:
        -------
        pd.DataFrame>
            A DataFrame containing the fetched data from the API.
        """
        data = self.get_prices()

        prices_list = []
        for market_hash_name, details in data.items():
            prices_data = details.get(self.SOURCES, {})
            if prices_data:
                prices_data["market_hash_name"] = market_hash_name
                prices_list.append(prices_data)

        df = pd.DataFrame(prices_list)

        return df


if __name__ == "__main__":
    csgo2 = CSGOS2kins()
    print(csgo2.get_prices_df())
