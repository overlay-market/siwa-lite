# Stdlib
from typing import Dict, Optional, List
from pydantic import BaseModel
import requests

# Third party
import pandas as pd

# Our stuff
from apis.base_skin_api import BaseAPI

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
    API_PREFIX : str
        Prefix for the API.
    PRICES_ENDPOINT : str
        Endpoint for prices in the API.
    PRICE_HISTORIES_ENDPOINT : str
        Endpoint for price histories in the API.
    QUANTITY_KEY : str
        Key for quantity in the DataFrame.
    QUANTITY_KEY_FOR_AGG : str
        Key for the aggregated quantity in the DataFrame.
    CURRENCY : str
        Currency used in the API.
    APP_ID : int
        Application ID for the API. Available values : 730, 440, 570, 252490 (Steam App id)
    SOURCES : List[str]
        List of sources for the API.
    DEFAULT_BASE_URL : str
        Default base URL for the API.
    DAYS : int
        Number of days needed for history data.

    Methods:
    --------
    extract_api_data(model: BaseModel, data):
       Extracts the relevant data from the API response.
    get_prices(range=DEFAULT_RANGE, agg=DEFAULT_AGG):
        Fetches the current prices of CSGO skins from the API.
    get_prices_df(range=DEFAULT_RANGE, agg=DEFAULT_AGG):
        Fetches the prices of CSGO skins and returns them as a pandas DataFrame.
    """

    API_PREFIX: str = "PRICE_EMPIRE"
    PRICES_ENDPOINT: str = "v3/items/prices"
    PRICE_HISTORIES_ENDPOINT: str = "v3/items/prices/history"
    QUANTITY_KEY: str = "quantity"
    QUANTITY_KEY_FOR_AGG: str = "count"
    CURRENCY: str = "USD"
    # Available values : 730, 440, 570, 252490 (Steam App id)
    APP_ID: int = 730
    SOURCES: List[str] = [
        "buff_rmb",
        "cs2go",
        "buff",
        "csgoempire_coins",
        "shadowpay",
        "csgotm",
        "whitemarket",
        "csgoempire",
        "skinport",
        "buffmarket",
        "c5game",
        "c5game_rmb",
        "haloskins",
    ]
    DEFAULT_BASE_URL: str = "https://api.pricempire.com/"
    DAYS: int = 7  # Need for History data

    def extract_api_data(self, data: dict, model) -> dict:
        """
        Extracts the relevant data from the API response.

        Parameters:
        -----------
        data : dict
            The data pulled from the API.

        Returns:
        -------
        dict
            The relevant data from the API response.
        """
        for market_hash_name, item in data.items():
            model(data={market_hash_name: item})

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
        response: requests.Response = requests.get(url, headers=headers, params=payload)
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

        prices_list = []

        for market_hash_name, details in data.items():
            for source in self.SOURCES:
                prices_data: dict = details.get(source, {})
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
    df: pd.DataFrame = pe.agg_data(df, pe.QUANTITY_KEY_FOR_AGG)
    caps: pd.DataFrame = pe.get_caps(df, k=100)
    index: float = pe.get_index(df, caps)
