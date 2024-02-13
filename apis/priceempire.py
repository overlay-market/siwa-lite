try:
    from apis.utils import get_api_key
except ModuleNotFoundError:
    from utils import get_api_key
from pydantic import BaseModel, ValidationError
import requests
from typing import Dict, Optional, List
import pandas as pd
import numpy as np
import prometheus_metrics


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


class CSGOS2kinsPrices(BaseModel):
    prices: Dict[str, Skin]


class PriceHistory(BaseModel):
    item_name: str
    prices: List[int]

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
    MAPPING_PATH = "csgo/csgo_mapping.csv" #TODO - fix unmaintainable relative path  
    MARKET_HASH_NAME_KEY = "market_hash_name"
    QUANTITY_MAP_KEY = "mapped_quantity"
    PRICE_KEY = "price"
    QUANTITY_KEY = "count"
    QUANTITY_KEY_MAPPING = "quantity"
    APP_ID = 730  # Available values : 730, 440, 570, 252490 (Steam App id)
    SOURCES = "cs2go"
    DEFAULT_BASE_URL = "https://api.pricempire.com/"
    DAYS = 7 # Need for History data 
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

    # def validate_api_data(self, model: BaseModel, data):
    #     """
    #     Validate data pulled from external API using Pydantic.

    #     Parameters:
    #     -----------
    #     model : pydantic.BaseModel
    #         The Pydantic model to validate against.
    #     data : dict
    #         The data pulled from the API.

    #     Raises:
    #     -------
    #     Exception
    #         If the data does not match the pre-defined Pydantic data structure.

    #     """
    #     try:
    #         for market_hash_name, item in data.items():
    #             model(prices={market_hash_name: item})
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
            "api_key": self.api_key,
            "currency": self.CURRENCY,
            "appId": self.APP_ID,
            "sources": self.SOURCES,
        }
        response = requests.get(url, headers=self.headers, params=payload)
        data = response.json()
        # self.validate_api_data(CSGOS2kinsPrices, data)
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
        df[self.PRICE_KEY] = df[self.PRICE_KEY] / 100
        return df

    def agg_data(self, df):
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
                count=pd.NamedAgg(column=self.QUANTITY_KEY, aggfunc="sum"),
            )
            .reset_index()
        )
        for row in df.to_dict("records"):
            prometheus_metrics.csgo_price_gauge.labels(
                market_hash_name=row["market_hash_name"]
            ).set(row["price"])
        return df

    def get_caps(
        self,
        mapping: pd.DataFrame,
        k: float = None,
        upper_multiplier: float = None,
        lower_multiplier: float = None,
    ):
        """
        Derives the caps for each skin in the mapping.

        Parameters:
        ----------
        mapping : pd.DataFrame
            The input DataFrame containing skins and their avg and std dev of
            index share.
        upper_multiplier : float
            The multiplier to use for the upper cap.
        lower_multiplier : float
            The multiplier to use for the lower cap.

        Returns:
        -------
        pd.DataFrame
            Input dataframe with caps added.
        """
        # Get caps for each skin
        mapping = pd.read_csv(self.MAPPING_PATH, index_col=0)
        mapping = mapping.rename(
            columns={self.QUANTITY_KEY_MAPPING: self.QUANTITY_MAP_KEY}
        )
        if (k is None) and (upper_multiplier is None and lower_multiplier is None):
            raise ValueError("Must specify either k or upper/lower multipliers")
        if (k is not None) and (
            upper_multiplier is not None or lower_multiplier is not None
        ):
            raise ValueError("Cannot specify both k and upper/lower multipliers")
        if upper_multiplier is not None and lower_multiplier is None:
            mapping["upper_cap_index_share"] = (
                mapping["avg_index_share"]
                + upper_multiplier * mapping["std_index_share"]
            )
            mapping["lower_cap_index_share"] = np.where(
                mapping["avg_index_share"]
                - lower_multiplier * mapping["std_index_share"]
                > 0,
                mapping["avg_index_share"]
                - lower_multiplier * mapping["std_index_share"],
                0,
            )
        elif k is not None:
            mapping["multiplier"] = np.exp(-k * mapping["avg_index_share"])
            mapping["upper_cap_index_share"] = (
                mapping["avg_index_share"]
                + mapping["multiplier"] * mapping["std_index_share"]
            )
            mapping["lower_cap_index_share"] = (
                mapping["avg_index_share"]
                - mapping["multiplier"] * mapping["std_index_share"]
            )
            mapping["lower_cap_index_share"] = np.where(
                mapping["lower_cap_index_share"] < 0,
                0,
                mapping["lower_cap_index_share"],
            )
        return mapping

    def adjust_share(self, df, max_iter):
        # Initialize
        df["mean_cap_index_share"] = (
            df["lower_cap_index_share"] + df["upper_cap_index_share"]
        ) / 2
        elements = df.iloc[:, 0].tolist()
        min_percentages = df.loc[:, "lower_cap_index_share"].tolist()
        max_percentages = df.loc[:, "upper_cap_index_share"].tolist()
        mean_percentages = df.loc[:, "mean_cap_index_share"].tolist()
        max_iterations = max_iter  # setting a limit to prevent infinite loops
        iterations = 0

        while iterations < max_iterations:
            # Calculate total sum
            sum_elements = sum(elements)
            # Calculate deviations
            deviations = []
            for i, num in enumerate(elements):
                current_percentage = num / sum_elements
                if current_percentage < min_percentages[i]:
                    deviation = current_percentage - min_percentages[i]
                elif current_percentage > max_percentages[i]:
                    deviation = current_percentage - max_percentages[i]
                else:
                    deviation = 0
                deviations.append(deviation)

            # Check if all deviations are zero (all elements within their acceptable ranges)
            if all(d == 0 for d in deviations):
                break

            # Sort by deviation
            sorted_indices = sorted(
                range(len(deviations)), key=lambda k: abs(deviations[k]), reverse=True
            )

            # Adjust the Most Deviating Element
            most_deviating_index = sorted_indices[0]
            if deviations[most_deviating_index] < 0:
                # Below the acceptable range
                target_value = min_percentages[most_deviating_index] * sum_elements
            else:
                # Above the acceptable range
                target_value = max_percentages[most_deviating_index] * sum_elements

            # Update the value in elements list
            elements[most_deviating_index] = target_value

            iterations += 1

        # Update the DataFrame
        df.iloc[:, 0] = elements

        return df

    def get_index(self, df, caps):
        # Get caps
        df = df.merge(caps, on=self.MARKET_HASH_NAME_KEY, how='inner')
        df['index'] = df[self.PRICE_KEY] * df[self.QUANTITY_MAP_KEY]

        adjusted_df = self.adjust_share(
            df[['index', 'lower_cap_index_share', 'upper_cap_index_share']],
            max_iter=1000
        )
        index = adjusted_df['index'].sum()
        # # index = self.cap_compared_to_prev(index)

        # # Set prometheus metric to index
        print(f'Set prometheus metric to index {index}')
        prometheus_metrics.csgo_index_gauge.set(index)
        return index

if __name__ == "__main__":
    csgo2 = CSGOS2kins()
    data = csgo2.get_prices()
    df = csgo2.get_prices_df()
    df = csgo2.agg_data(df)
    caps = csgo2.get_caps(df, k=100)
    index = csgo2.get_index(df, caps)
    breakpoint()
    print("index: ", index)
