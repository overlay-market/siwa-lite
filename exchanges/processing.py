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
