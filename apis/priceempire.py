from typing import Dict, Optional, List
from pydantic import BaseModel, ValidationError
import requests
import pandas as pd
import prometheus_metrics
from base_skin_api import BaseAPI

try:
    from apis.utils import get_api_key
except ModuleNotFoundError:
    from utils import get_api_key


class Source(BaseModel):
    isInflated: Optional[bool] = None
    price: Optional[int] = None
    count: Optional[int] = None
    avg30: Optional[int] = None
    createdAt: Optional[str] = None


class Skin(BaseModel):
    liquidity: Optional[float] = None
    steam_volume: Optional[int] = None
    cs2go: Optional[Source] = None

    class Config:
        extra = "allow"


class PriceEmpirePrices(BaseModel):
    data: Dict[str, Skin]


class PriceHistory(BaseModel):
    item_name: str
    prices: List[int]


class PriceEmpire(BaseAPI):
    """
    A class to interact with the CSGOSkins API to fetch and process skin prices.

    Inherits from:
        BaseAPI: Base class for API operations.

    Attributes:
    ----------
    prices_endpoint : str
        The API endpoint for fetching current prices of CSGO skins.
    base_url : str
        The base URL for the CSGOSkins API.
    api_key : str
        The API key to authenticate with the CSGOSkins API.

    Methods:
    --------
    __init__():
        Initializes the CSGOSkins class with the base URL and API key.
    validate_api_data(model: BaseModel, data):
        Validate data pulled from external API using Pydantic.
    get_prices(range=DEFAULT_RANGE, agg=DEFAULT_AGG):
        Fetches the current prices of CSGO skins from the API.
    get_prices_df(range=DEFAULT_RANGE, agg=DEFAULT_AGG):
        Fetches the prices of CSGO skins and returns them as a pandas DataFrame.
    agg_data(df):
        Aggregates the data of a given DataFrame by 'market_hash_name'.
    """

    API_PREFIX: str = "PRICE_EMPIRE"
    PRICES_ENDPOINT: str = "v3/items/prices"
    PRICE_HISTORIES_ENDPOINT: str = "v3/items/prices/history"
    QUANTITY_KEY: str = "quantity"
    QUANTITY_KEY_AGG: str = "count"
    CURRENCY: str = "USD"
    APP_ID: int = 730  # Available values : 730, 440, 570, 252490 (Steam App id)
    SOURCES: str = "cs2go"
    DEFAULT_BASE_URL: str = "https://api.pricempire.com/"
    DAYS: int = 7  # Need for History data

    def validate_api_data(self, model: BaseModel, data: dict) -> None:
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
        try:
            for market_hash_name, item in data.items():
                model(data={market_hash_name: item})
        except ValidationError as e:
            raise Exception(
                f"Data pulled from {self.base_url} does not match "
                f"pre-defined Pydantic data structure: {e}"
            )

    def get_prices(self) -> dict:
        """
        Retrieves the current prices of CSGO skins from the API.

        Returns:
        -------
        dict
            A dictionary containing the retrieved data from the API.
        """
        url: str = self.DEFAULT_BASE_URL + self.PRICES_ENDPOINT
        payload: dict = {
            "api_key": get_api_key(self.API_PREFIX),
            "currency": self.CURRENCY,
            "appId": self.APP_ID,
            "sources": self.SOURCES,
        }
        headers = {self.CONTENT_TYPE_KEY: self.CONTENT_TYPE}
        response: requests.Response = requests.get(
            url, headers=headers, params=payload
        ) 
        data: dict = response.json()
        self.validate_api_data(PriceEmpirePrices, data)
        return data

    def get_prices_df(self) -> pd.DataFrame:
        """
        Fetches the prices of CSGO skins and returns them as a pandas
        DataFrame.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the fetched data from the API.
        """
        data: dict = self.get_prices()

        prices_list: List[dict] = []
        for market_hash_name, details in data.items():
            prices_data: dict = details.get(self.SOURCES, {})
            if prices_data:
                prices_data["market_hash_name"] = market_hash_name
                prices_list.append(prices_data)

        df: pd.DataFrame = pd.DataFrame(prices_list)
        df[self.PRICE_KEY] = df[self.PRICE_KEY] / 100
        return df

if __name__ == "__main__":
    pe: PriceEmpire = PriceEmpire()
    data: dict = pe.get_prices()
    df: pd.DataFrame = pe.get_prices_df()
    df: pd.DataFrame = pe.agg_data(df, pe.QUANTITY_KEY_AGG)
    caps: pd.DataFrame = pe.get_caps(df, k=100)
    index: float = pe.get_index(df, caps)
