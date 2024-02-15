from typing import Optional
import pandas as pd
import numpy as np
import prometheus_metrics


class BaseAPI:
    """
    Base class for API operations.

    Attributes:
    -----------
    PRICE_KEY : str
        Key for price in the DataFrame.
    MAPPING_PATH : str
        Path to the CSV file containing the mapping.
    QUANTITY_MAP_KEY : str
        Key for the mapped quantity in the DataFrame.
    QUANTITY_KEY : str
        Key for quantity in the DataFrame.
    MARKET_HASH_NAME_KEY : str
        Key for market hash name in the DataFrame.
    CONTENT_TYPE_KEY : str
        Key for content type in the request header.
    CONTENT_TYPE : str
        Value for content type in the request header.

    Methods:
    --------
    __init__(base_url: Optional[str] = None)
        Initializes the BaseAPI object with a base URL.
    get_caps(mapping: pd.DataFrame, k: float = None, upper_multiplier: float = None, lower_multiplier: float = None)
        Retrieves caps for each skin in the mapping DataFrame.
    adjust_share(self, df, max_iter)
        Adjusts the share of each element within the dataframe to ensure they fall within the specified range.
    get_index(self, df, caps)
        Derives the index for each skin in the DataFrame based on caps.
    """

    PRICE_KEY = "price"
    MAPPING_PATH = "csgo/csgo_mapping.csv"
    QUANTITY_MAP_KEY = "mapped_quantity"
    QUANTITY_KEY = "quantity"
    MARKET_HASH_NAME_KEY = "market_hash_name"
    CONTENT_TYPE_KEY = "Content-Type"
    CONTENT_TYPE = "application/json"

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url

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

    def adjust_share(self, df, max_iter):
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

    def get_index(self, df, caps):
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
