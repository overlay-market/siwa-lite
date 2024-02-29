import json

import numpy as np
import pandas as pd

from exchanges.constants.utils import SPREAD_MIN, SPREAD_MULTIPLIER, RANGE_MULT


class Processing:
    @staticmethod
    def calculate_implied_interest_rates(df):
        # Calculate implied interest rates
        df = df.copy()

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
    def normalize_type(df):
        df = df.copy()
        df["symbol"] = df["symbol"].astype(str)
        df["bid"] = df["bid"].astype(float)
        df["ask"] = df["ask"].astype(float)
        df["mark_price"] = df["mark_price"].astype(float)
        df["underlying_price"] = df["underlying_price"].astype(float)
        df["bid_spread"] = df["bid_spread"].astype(float)
        df["ask_spread"] = df["ask_spread"].astype(float)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["expiry"] = pd.to_datetime(df["expiry"])
        df["strike_price"] = df["strike_price"].astype(int)
        df["YTM"] = df["YTM"].astype(float)
        df["rimp"] = df["rimp"].astype(float)
        return df

    @staticmethod
    def consolidate_quotes(df):
        df["spread"] = df["ask"] - df["bid"]
        grouped = df.groupby(["symbol", "strike_price", "expiry", "option_type"])

        agg_funcs = {
            "bid": "max",
            "ask": "min",
            "mark_price": "min",
            "underlying_price": "min",
            "bid_spread": "min",
            "ask_spread": "min",
            "datetime": "min",
            "datetime_hum": "min",
            "YTM": "min",
            "rimp": "min",
        }

        consolidated = grouped.agg(agg_funcs).reset_index()
        consolidated.to_json("consolidated.json", orient="records", indent=4)
        return consolidated

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
