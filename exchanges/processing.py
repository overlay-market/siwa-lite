import json

import numpy as np
import pandas as pd


class Processing:
    def preprocess_data(self, df):
        # Convert timestamps to datetime objects
        # it takes format:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")

        current_date = pd.Timestamp.now()

        # Apply the function to extract and parse the expiry date
        df["expiry"] = df["symbol"].apply(self.date_parser)

        df["YTM"] = (df["expiry"] - current_date) / np.timedelta64(1, "Y")

        return df

    @staticmethod
    def date_parser(symbol):
        # Define a list of possible date formats to try
        date_formats = [
            "%y%m%d",  # YYMMDD
            "%d%b%y",  # DDMMMYY
            "%Y%m%d",  # YYYYMMDD
            "%m%d%Y",  # MMDDYYYY
            "%d%m%Y",  # DDMMYYYY
        ]

        # Split the symbol based on common separators
        parts = symbol.replace(":", "-").replace("/", "-").replace(".", "-").split("-")
        date_str = None

        # Loop through the parts to identify potential date segment
        for part in parts:
            # Skip segments that are purely alphabetical, as they likely don't contain date info
            if part.isalpha():
                continue
            # Try to parse each segment with each date format until successful
            for date_format in date_formats:
                try:
                    date = pd.to_datetime(part, format=date_format)
                    return date  # Return the parsed date as soon as a successful parse occurs
                except ValueError:
                    continue  # If parsing fails, try the next format

        return pd.NaT  # Return Not-A-Time if no date could be parsed

    @staticmethod
    def calculate_implied_interest_rates(df):
        # Calculate implied interest rates
        df["rimp"] = (
            np.log(df["estimated_delivery_price"] / df["underlying_price"])
        ) / df["YTM"]
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
