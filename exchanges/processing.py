import json

import numpy as np
import pandas as pd

from exchanges.constants.utils import SPREAD_MIN, SPREAD_MULTIPLIER, RANGE_MULT


class Processing:
    @staticmethod
    def calculate_yield_curve(dataframe):
        """
        Calculates the average interest rate for each expiry date in a pandas DataFrame.

        Parameters:
        - dataframe: A pandas DataFrame containing at least two columns: 'expiry' and 'implied_interest_rate'.

        Returns:
        - A pandas DataFrame containing the average implied interest rate for each unique expiry date.
        """
        dataframe = dataframe.sort_values(by="expiry", ascending=False)

        grouped = (
            dataframe.groupby("expiry")["implied_interest_rate"].mean().reset_index()
        )

        return grouped[
            ["expiry", "implied_interest_rate", "days_to_expiry", "years_to_expiry"]
        ]

    @staticmethod
    def build_interest_rate_term_structure(df):
        # Group by expiry date and calculate the average implied interest rate for each expiry
        interest_rate_term_structure = df.groupby("expiry")["rimp"].mean().reset_index()

        # Rename columns for clarity
        interest_rate_term_structure.rename(
            columns={"rimp": "average_implied_interest_rate"}, inplace=True
        )

        return interest_rate_term_structure

    @staticmethod
    def near_and_next_term_options(df):
        df = df.copy()

        df["datetime"] = pd.to_datetime(df["datetime"])
        df["expiry"] = pd.to_datetime(df["expiry"])
        df = df.sort_values(by=["expiry", "datetime"])
        near_term = df[df["expiry"] <= df["datetime"].max()]
        next_term = df[df["expiry"] > df["datetime"].max()]

        return near_term, next_term

    @staticmethod
    def eliminate_invalid_quotes(df):
        df = df[
            (df["ask"] > df["bid"])
            & (df["mark_price"] >= df["bid"])
            & (df["mark_price"] <= df["ask"])
            & (df["mark_price"] > 0)
        ]
        return df

    @staticmethod
    def process_quotes(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Calculate spreads
        df["bid_spread"] = df["mark_price"] - df["bid"]
        df["ask_spread"] = df["ask"] - df["mark_price"]

        # Set spreads to zero if negative
        df["bid_spread"] = df["bid_spread"].apply(lambda x: x if x > 0 else 0)
        df["ask_spread"] = df["ask_spread"].apply(lambda x: x if x > 0 else 0)

        # Calculate total spread
        df["spread"] = df["bid_spread"] + df["ask_spread"]

        # Calculate MAS
        df["MAS"] = df[["bid_spread", "ask_spread"]].min(axis=1) * SPREAD_MULTIPLIER

        # Calculate GMS
        GMS = SPREAD_MIN * SPREAD_MULTIPLIER

        df = df[(df["spread"] <= GMS) | (df["spread"] <= df["MAS"])]

        # Extract strike price and option type (Call/Put) from the symbol
        df["strike"] = df["symbol"].apply(lambda x: int(x.split("-")[2]))
        df["option_type"] = df["symbol"].apply(lambda x: x[-1])

        df["mid_price"] = (df["bid"] + df["ask"]) / 2
        return df

    @staticmethod
    def calculate_implied_forward_price(df):
        YTM = 30 / 365  # Approximation of 0.082192 years

        df_c = df[df["option_type"] == "C"].set_index("strike")
        df_p = df[df["option_type"] == "P"].set_index("strike")

        df_joined = df_c.join(df_p, lsuffix="_C", rsuffix="_P")

        df_joined["mid_price_diff"] = abs(
            df_joined["mid_price_C"] - df_joined["mid_price_P"]
        )
        min_diff_row = df_joined.loc[df_joined["mid_price_diff"].idxmin()]

        Fimp = min_diff_row.name + YTM * (
            min_diff_row["mid_price_C"] - min_diff_row["mid_price_P"]
        )

        return Fimp, min_diff_row.name

    @staticmethod
    def select_otm_options(df, Fimp):
        # Identify ATM strike
        KATM = df[df["strike"] < Fimp]["strike"].max()

        # Select OTM options: For calls, strike > KATM; For puts, strike < KATM
        otm_calls = df[(df["strike"] > KATM) & (df["option_type"] == "C")]
        otm_puts = df[(df["strike"] < KATM) & (df["option_type"] == "P")]

        # Calculate average mid-price for KATM if applicable
        if KATM in df["strike"].values:
            avg_mid_price_KATM = df[df["strike"] == KATM]["mid_price"].mean()
        else:
            avg_mid_price_KATM = None

        return KATM, otm_calls, otm_puts, avg_mid_price_KATM

    @staticmethod
    def filter_and_sort_options(df, Fimp, RANGE_MULT=2.5, min_bid_threshold=0.01):
        """
        Filters options based on dynamically calculated Kmin and Kmax from Fimp and RANGE_MULT.
        Then sorts the remaining options by strike and eliminates options after observing
        five consecutive bid prices <= min_bid_threshold.

        :param df: DataFrame containing options data.
        :param Fimp: Calculated implied forward price.
        :param RANGE_MULT: Multiplier to set the range of strike prices.
        :param min_bid_threshold: Minimum bid threshold, equal to tick size.
        :return: Filtered and sorted DataFrame.
        """
        Kmin = Fimp / RANGE_MULT
        Kmax = Fimp * RANGE_MULT

        filtered_df = df[(df["strike"] > Kmin) & (df["strike"] < Kmax)]

        sorted_df = filtered_df.sort_values(by="strike")

        consecutive_low_bids = 0
        indices_to_drop = []

        for index, row in sorted_df.iterrows():
            if row["bid"] <= min_bid_threshold:
                consecutive_low_bids += 1
                if consecutive_low_bids >= 5:
                    indices_to_drop.append(index)
            else:
                consecutive_low_bids = 0

        sorted_filtered_df = sorted_df.drop(indices_to_drop)

        return sorted_filtered_df

    @staticmethod
    def calculate_raw_implied_variance(df, Fi, Ki_ATM, Ti, r):
        """
        Calculate the raw implied variance for options.

        :param df: DataFrame containing options data, expected to be sorted and filtered.
        :param Fi: Implied forward price.
        :param Ki_ATM: ATM strike level.
        :param Ti: Time to maturity in years.
        :param r: Annual risk-free interest rate.
        :return: Raw implied variance.
        """
        # Ensure df is sorted by strike
        df_sorted = df.sort_values(by="strike")

        # Calculate delta K for each option, assuming equidistant strikes post-interpolation
        df_sorted["delta_K"] = (
            df_sorted["strike"].diff().fillna(method="bfill").astype(float)
        )

        # Calculate weights
        df_sorted["wi"] = np.exp(r * Ti) * (
            df_sorted["delta_K"] / df_sorted["strike"] ** 2
        )

        # Calculate the variance contribution for each option
        df_sorted["variance_contribution"] = (
            2 * df_sorted["wi"] * df_sorted["mid_price"]
        )

        # Sum up the variance contributions
        sum_variance_contributions = df_sorted["variance_contribution"].sum()

        # Calculate the raw implied variance
        raw_implied_variance = (
            sum_variance_contributions - ((Fi / Ki_ATM) - 1) ** 2
        ) / Ti

        return raw_implied_variance

    @staticmethod
    def atm_strike(df):
        df = df.copy()
        df["KATM"] = df[df["strike"] < df["Fimp"]].groupby("expiry")["strike"].max()
        return df

    def process_global_orderbook(self, df):
        df = self.normalize_type(df)
        df = self.consolidate_quotes(df)
        df = self.convert_datetimes_and_calculate_ytm(df)
        df = self.calculate_mid_prices(df)
        df = self.calculate_implied_forward_price(df)
        df = self.find_atm_strike(df)
        df_otm = self.filter_otm_options_and_set_prices(df)
        return df

    @staticmethod
    def consolidate_quotes(df):
        df = (
            df.groupby("symbol")
            .agg(
                {
                    "bid": "mean",
                    "ask": "mean",
                    "mark_price": "mean",
                }
            )
            .reset_index()
        )
        return df

    @staticmethod
    def convert_datetimes_and_calculate_ytm(df):
        # Convert datetime from milliseconds if needed and calculate YTM
        df["expiry"] = pd.to_datetime(df["expiry"], unit="ms")
        df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
        df["YTM_calc"] = 30 / 365  # Fixed value based on the description
        return df

    @staticmethod
    def calculate_mid_prices(df):
        df["mid_price"] = (df["ask"] + df["bid"]) / 2
        return df

    @staticmethod
    def calculate_implied_forward_price(df):
        df["Fimp"] = (
            df["strike_price"].astype(float) + df["mid_price"]
        )  # Simplify and adjust as needed
        return df

    @staticmethod
    def find_atm_strike(df):
        # Determine ATM strike for each expiry, this is also simplified
        df["KATM"] = df.groupby("expiry")["Fimp"].transform(
            lambda x: x[x < x.max()].max()
        )
        return df

    @staticmethod
    def filter_otm_options_and_set_prices(df):
        # Filter for OTM options and set prices, adjusting for your actual logic
        df_otm = df[df["strike_price"].astype(float) > df["KATM"]]
        df_otm["price"] = df_otm[
            "mid_price"
        ]  # Assuming this is correct; adjust if needed
        return df_otm
