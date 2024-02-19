# Stdlib
from typing import Optional
import prometheus_metrics
from pydantic import BaseModel, ValidationError
import os

# Third party
import pandas as pd
import numpy as np

# Out stuff
import constants as c


class BaseAPI:
    """
    Base class for Skins Apis.

    Attributes:
    -----------
    PRICE_KEY : str
        Key for price in the DataFrame.
    MAPPING_PATH : str
        Path to the CSV file containing the mapping.
    QUANTITY_MAP_KEY : str
        Key for the mapped quantity in the DataFrame.
    MARKET_HASH_NAME_KEY : str
        Key for market hash name in the DataFrame.
    CONTENT_TYPE_KEY : str
        Key for content type in the request header.
    CONTENT_TYPE : str
        Value for content type in the request header.

    Methods:
    --------
    validate_api_data(model: BaseModel, data):
        Validate data pulled from external API using Pydantic.
    agg_data(df, quantity_key):
        Aggregates the data of a given DataFrame by 'market_hash_name'.
    get_caps(mapping: pd.DataFrame, k: float = None, upper_multiplier: float = None, lower_multiplier: float = None) -> pd.DataFrame
        Retrieves caps for each skin in the mapping DataFrame.
    adjust_share(self, df: pd.DataFrame, max_iter: int) -> pd.DataFrame
        Adjusts the share of each element within the dataframe to ensure they fall within the specified range.
    get_index(self, df: pd.DataFrame, caps: pd.DataFrame) -> float
        Derives the index for each skin in the DataFrame based on caps.
    """

    PRICE_KEY: str = "price"
    MAPPING_PATH: str = os.path.join(c.DATA_DIR, "csgo/csgo_mapping.csv")
    QUANTITY_MAP_KEY: str = "mapped_quantity"
    MARKET_HASH_NAME_KEY: str = "market_hash_name"
    CONTENT_TYPE_KEY: str = "Content-Type"
    CONTENT_TYPE: str = "application/json"

    def validate_api_data(cls, model: BaseModel, data: dict) -> None:
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
            cls.extract_api_data(data, model)
        except ValidationError as e:
            raise Exception(f"pre-defined Pydantic data structure: {e}")

    def agg_data(self, df: pd.DataFrame, quantity_key: str) -> pd.DataFrame:
        """
        Aggregates the data of a given DataFrame by 'market_hash_name',
        computing the minimum price and total quantity for each group.

        Parameters:
        ----------
        df : pd.DataFrame
            The input DataFrame containing the data to aggregate.
        quantity_key : str
            The column name for the quantity in the DataFrame.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the aggregated data.
        """

        df: pd.DataFrame = (
            df.groupby(self.MARKET_HASH_NAME_KEY)[self.PRICE_KEY, quantity_key]
            .agg(
                price=pd.NamedAgg(column=self.PRICE_KEY, aggfunc="min"),
                quantity_key=pd.NamedAgg(column=quantity_key, aggfunc="sum"),
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
        k: Optional[float] = None,
        upper_multiplier: Optional[float] = None,
        lower_multiplier: Optional[float] = None,
    ) -> pd.DataFrame:
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
        mapping = mapping.rename(columns={self.QUANTITY_KEY: self.QUANTITY_MAP_KEY})
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

    def adjust_share(self, df: pd.DataFrame, max_iter: int) -> pd.DataFrame:
        """
        Adjusts the share of each element within the dataframe to ensure they fall within the specified range.

        Parameters:
        ----------
        df : pd.DataFrame
            The input DataFrame containing index shares and their upper and lower bounds.
        max_iter : int
            Maximum number of iterations to adjust shares.

        Returns:
        -------
        pd.DataFrame
            DataFrame with adjusted shares.
        """
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

    def get_index(self, df: pd.DataFrame, caps: pd.DataFrame) -> float:
        """
        Derives the index for each skin in the DataFrame based on caps.

        Parameters:
        ----------
        df : pd.DataFrame
            DataFrame containing skins and their corresponding price and quantity map.
        caps : pd.DataFrame
            DataFrame containing caps for each skin.

        Returns:
        -------
        float
            Calculated index value.
        """
        df = df.merge(caps, on=self.MARKET_HASH_NAME_KEY, how="inner")
        df["index"] = df[self.PRICE_KEY] * df[self.QUANTITY_MAP_KEY]
        adjusted_df = self.adjust_share(
            df[["index", "lower_cap_index_share", "upper_cap_index_share"]],
            max_iter=1000,
        )
        index = adjusted_df["index"].sum()
        # index = self.cap_compared_to_prev(index)

        # Set prometheus metric to index
        print(f"Set prometheus metric to index {index}")
        prometheus_metrics.csgo_index_gauge.set(index)
        return index
