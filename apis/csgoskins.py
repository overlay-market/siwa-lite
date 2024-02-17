from typing import Optional, Dict, List
import pandas as pd
import numpy as np
import requests
from pydantic import BaseModel, ValidationError
from web3 import Web3
import json
from base_skin_api import BaseAPI
import prometheus_metrics

try:
    from apis.utils import get_api_key
except ModuleNotFoundError:
    from utils import get_api_key


class CSGOSkinsPrice(BaseModel):
    market: str
    price: int
    quantity: int
    updated_at: Optional[int]


class CSGOSkinsPrices(BaseModel):
    market_hash_name: str
    prices: List[CSGOSkinsPrice]


class CSGOSkinsPriceHistoryDate(BaseModel):
    date: str
    prices: List[CSGOSkinsPrice]


class CSGOSkinsPriceHistory(BaseModel):
    market_hash_name: str
    dates: List[CSGOSkinsPriceHistoryDate]


class CSGOSkinsPriceHistories(BaseModel):
    meta: Dict
    data: List[CSGOSkinsPriceHistory]


class CSGOSkins(BaseAPI):
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
    cap_compared_to_prev(index):
        Caps the index to be within 5% of the previous index.
    """

    API_PREFIX: str = "CSGO"
    PRICES_ENDPOINT: str = "api/v1/prices"
    PRICE_HISTORIES_ENDPOINT: str = "api/v1/price-histories"
    PRICE_HISTORIES_RPM: int = 20
    DEFAULT_BASE_URL: str = "https://csgoskins.gg/"
    DEFAULT_RANGE: str = "current"
    DEFAULT_AGG: str = "max"
    AUTH_TYPE: str = "Bearer"
    RANGE_KEY: str = "range"
    AGGREGATOR_KEY: str = "aggregator"
    DATA_KEY: str = "data"
    PRICES_KEY: str = "prices"
    AUTHORIZATION_KEY: str = "Authorization"
    GOERLI_URL: str = "https://goerli.infura.io/v3/"
    INFURA_PREFIX: str = "INFURA"
    CONTRACT_ADD_FILE: str = "apis/csgo/contract_address.txt"
    ABI_FILE: str = "apis/csgo/abi.json"

    def validate_api_data(self, model: BaseModel, data) -> None:
        """Validate data pulled from external API using Pydantic."""
        try:
            for item in data:
                model(**item)
        except ValidationError as e:
            raise Exception(
                f"Data pulled from {self.base_url} does not match "
                f"pre-defined Pydantic data structure: {e}"
            )

    def get_prices(self, range: str = DEFAULT_RANGE, agg: str = DEFAULT_AGG) -> dict:
        """
        Fetches the current prices of CSGO skins from the API.

        Parameters:
        ----------
        range : str, optional
            The range of prices to fetch. Default is 'current'.
        agg : str, optional
            The aggregation method to use. Default is 'max'. As per API docs,
            this is ignored if `range` is set to "current".

        Returns:
        -------
        dict
            A dictionary containing the fetched data from the API.
        """
        url: str = self.DEFAULT_BASE_URL + self.PRICES_ENDPOINT
        payload: dict = {
            self.RANGE_KEY: range, 
            self.AGGREGATOR_KEY: agg
        }
        api_key = get_api_key(self.API_PREFIX)
        headers = {self.AUTHORIZATION_KEY: f"{self.AUTH_TYPE} {api_key}"}
        response: requests.Response = requests.request(
            "GET", url, headers=headers, json=payload
        )
        data: dict = response.json()
        self.validate_api_data(CSGOSkinsPrices, data["data"])
        return data

    def get_prices_df(self) -> pd.DataFrame:
        """
        Fetches the prices of CSGO skins and returns them as a pandas
        DataFrame.

        Parameters:
        ----------
        range : str, optional
            The range of prices to fetch. Default is 'current'. As per API
            docs, this is ignored if `range` is set to "current".
        agg : str, optional
            The aggregation method to use. Default is 'max'.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the fetched data from the API.
        """
        data: dict = self.get_prices()
        df: pd.DataFrame = pd.json_normalize(
            data[self.DATA_KEY],
            record_path=self.PRICES_KEY,
            meta=self.MARKET_HASH_NAME_KEY,
        )
        df[self.PRICE_KEY] = df[self.PRICE_KEY] / 100
        return df

    def agg_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates the data of a given DataFrame by 'market_hash_name',
        computing the minimum price and total quantity for each group.

        Parameters:
        ----------
        df : pd.DataFrame
            The input DataFrame containing the data to aggregate.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the aggregated data.
        """
        df = (
            df.groupby(self.MARKET_HASH_NAME_KEY)[self.PRICE_KEY, self.QUANTITY_KEY]
            .agg(
                price=pd.NamedAgg(column=self.PRICE_KEY, aggfunc="min"),
                quantity=pd.NamedAgg(column=self.QUANTITY_KEY, aggfunc="sum"),
            )
            .reset_index()
        )
        for row in df.to_dict("records"):
            prometheus_metrics.csgo_price_gauge.labels(
                market_hash_name=row["market_hash_name"]
            ).set(row["price"])
        return df

    def cap_compared_to_prev(self, index: float) -> float:
        """
        Caps the index to be within 5% of the previous index.

        Parameters:
        ----------
        index : float
            The current index.

        Returns:
        -------
        float
            The capped index.
        """
        # Read answer from chainlink contract using web3py
        # Get contract address from file
        with open(self.CONTRACT_ADD_FILE) as f:
            contract_address: str = f.read()
        # Read contract abi from file
        with open(self.ABI_FILE) as f:
            abi: dict = json.load(f)
        # Init contract
        contract = self.w3.eth.contract(address=contract_address, abi=abi)
        # Get answer
        prev_index: float = contract.functions.latestAnswer().call()
        decimals: int = contract.functions.decimals().call()
        prev_index = prev_index / 10**decimals
        # If prev_index more/less by 5% of index, cap current index at 5% move
        if index < prev_index * 0.95:
            index = prev_index * 0.95
        elif index > prev_index * 1.05:
            index = prev_index * 1.05
        return index


if __name__ == "__main__":
    csgo = CSGOSkins()
    data: dict = csgo.get_prices()
    df: pd.DataFrame = csgo.get_prices_df()
    df: pd.DataFrame = csgo.agg_data(df)
    caps: pd.DataFrame = csgo.get_caps(df, k=100)
    index: float = csgo.get_index(df, caps)
    print("index: ", index)
