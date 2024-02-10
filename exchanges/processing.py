import json

import numpy as np
import pandas as pd


class Processing:
    @staticmethod
    def calculate_implied_interest_rates(df):
        # Calculate implied interest rates
        df["rimp"] = (
            np.log(df["forward_price"]) - np.log(df["underlying_price"])
        ) / df["YTM"]

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
        df["bid_spread"] = (df["mark_price"] - df["bid"]).clip(lower=0)
        df["ask_spread"] = (df["ask"] - df["mark_price"]).clip(lower=0)
        df["spread"] = df["bid_spread"] + df["ask_spread"]
        return df

    @staticmethod
    def remove_large_spreads(df, spread_multiplier=1, spread_min=0):
        df["max_spread"] = (
            df[["bid_spread", "ask_spread"]].min(axis=1) * spread_multiplier
        )
        df = df[df["spread"] <= df["max_spread"]]
        df["global_max_spread"] = spread_min * spread_multiplier
        df = df[df["spread"] <= df["global_max_spread"]]
        return df

    @staticmethod
    def calculate_mid_price(df):
        df["mid_price"] = (df["bid"] + df["ask"]) / 2
        return df

    @staticmethod
    def calculate_interest_rate(df):
        df["YTM"] = (df["expiry"] - df["datetime"]) / np.timedelta64(1, "Y")
        df["rimp"] = (
            np.log(df["forward_price"]) - np.log(df["underlying_price"])
        ) / df["YTM"]
        return df
