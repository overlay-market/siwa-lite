import json

import numpy as np
import pandas as pd

from exchanges.constants.utils import SPREAD_MIN, SPREAD_MULTIPLIER, RANGE_MULT


class Processing:
    @staticmethod
    def calculate_implied_interest_rates(df):
        # Calculate implied interest rates
        df = df.copy()

        df["rimp"] = (np.log(df["mark_price"]) - np.log(df["underlying_price"])) / df[
            "YTM"
        ]

        # Rimp = (ln F − ln S)/T
        return df

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
    def consolidate_quotes(df):
        df = df.groupby(["type", "strike", "expiry"]).agg(
            {
                "bid": "max",
                "ask": "min",
                "mark_price": "min",
                "datetime": "min",
            }
        )
        return df

    @staticmethod
    def calculate_spreads(df):
        df = df.copy()
        df["bid_spread"] = (df["mark_price"] - df["bid"]).clip(lower=0)
        df["ask_spread"] = (df["ask"] - df["mark_price"]).clip(lower=0)
        df["spread"] = df["bid_spread"] + df["ask_spread"]
        return df

    @staticmethod
    def remove_large_spreads(df):
        df = df.copy()
        df["max_spread"] = (
            df[["bid_spread", "ask_spread"]].min(axis=1) * SPREAD_MULTIPLIER
        )
        df = df[df["spread"] <= df["max_spread"]]
        df["global_max_spread"] = SPREAD_MIN * SPREAD_MULTIPLIER
        df = df[df["spread"] <= df["global_max_spread"]]
        return df

    @staticmethod
    def calculate_mid_price(df):
        df["mid_price"] = (df["bid"] + df["ask"]) / 2
        return df

    @staticmethod
    def select_options_within_range(df):
        df = df.copy()
        df["Kmin"] = df["Fimp"] / RANGE_MULT
        df["Kmax"] = df["Fimp"] * RANGE_MULT
        df = df[(df["strike"] >= df["Kmin"]) & (df["strike"] <= df["Kmax"])]
        return df

    # Calculate the implied forward price of the strike that has minimum ab-
    # solute mid-price difference between call and put options, for near and
    # next-term options: Fimp = K + F × (C − P ) where F is the forward price,
    # C is the call option price, P is put option price, and both options are
    # quoted in the amounts of underlying.

    def implied_forward_price(self, df):
        df = df.copy()
        df = self.calculate_mid_price(df)
        df = self.select_options_within_range(df)
        df["Fimp"] = df["strike"] + df["F"] * (df["call"] - df["put"])
        return df

    # Set the largest strike that is less than the implied forward Fimp as ATM
    # strike KAT M for near and next-term options.
    @staticmethod
    def atm_strike(df):
        df = df.copy()
        df["KATM"] = df[df["strike"] < df["Fimp"]].groupby("expiry")["strike"].max()
        return df
