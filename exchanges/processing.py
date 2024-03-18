import json
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.interpolate import interp1d

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
    def filter_near_next_term_options(df):
        df['expiry'] = df['symbol'].apply(lambda x: datetime.strptime(x.split('-')[1], '%y%m%d'))
        index_maturity_days = 30
        today = datetime.now()
        near_term_options = df[(df['expiry'] - today).dt.days <= index_maturity_days]
        next_term_options = df[(df['expiry'] - today).dt.days > index_maturity_days]
        return near_term_options, next_term_options


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
        df["bid_spread"] = df["mark_price"] - df["bid"]
        df["ask_spread"] = df["ask"] - df["mark_price"]

        df["bid_spread"] = df["bid_spread"].apply(lambda x: x if x > 0 else 0)
        df["ask_spread"] = df["ask_spread"].apply(lambda x: x if x > 0 else 0)

        # Calculate total spread
        df["spread"] = df["bid_spread"] + df["ask_spread"]


        MAS = df[["bid_spread", "ask_spread"]].min(axis=1) * SPREAD_MULTIPLIER

        # Calculate GMS
        GMS = SPREAD_MIN * SPREAD_MULTIPLIER

        df = df[(df["spread"] <= GMS) | (df["spread"] <= MAS)]

        df["strike"] = df["symbol"].apply(lambda x: int(x.split("-")[2]))
        df["option_type"] = df["symbol"].apply(lambda x: x[-1])

        df["mid_price"] = (df["bid"] + df["ask"]) / 2
        return df

    @staticmethod
    def calculate_implied_forward_price(df):
        calls = df[df['option_type'] == 'C']
        puts = df[df['option_type'] == 'P']
        combined = calls[['strike', 'mid_price']].merge(puts[['strike', 'mid_price']], on='strike',
                                                        suffixes=('_call', '_put'))
        combined['mid_price_diff'] = abs(combined['mid_price_call'] - combined['mid_price_put'])
        min_diff_strike = combined.loc[combined['mid_price_diff'].idxmin()]
        forward_price = df.loc[df['strike'] == min_diff_strike['strike'], 'mark_price'].iloc[0]
        Fimp = min_diff_strike['strike'] + forward_price * (
                    min_diff_strike['mid_price_call'] - min_diff_strike['mid_price_put'])
        return Fimp

    @staticmethod
    def filter_and_sort_options(df, Fimp):
        KATM = df[df['strike'] < Fimp]['strike'].max()
        RANGE_MULT = 2.5
        Kmin = Fimp / RANGE_MULT
        Kmax = Fimp * RANGE_MULT
        calls_otm = df[(df['strike'] > KATM) & (df['option_type'] == 'C')]
        puts_otm = df[(df['strike'] < KATM) & (df['option_type'] == 'P')]
        otm_combined = pd.concat([calls_otm, puts_otm])
        otm_filtered = otm_combined[(otm_combined['strike'] > Kmin) & (otm_combined['strike'] < Kmax)]
        otm_sorted = otm_filtered.sort_values(by='strike')
        tick_size = df[df['bid'] > 0]['bid'].min()
        consecutive_threshold = 5
        consecutive_count = 0
        to_drop = []
        for index, row in otm_sorted.iterrows():
            if row['bid'] <= tick_size:
                consecutive_count += 1
                to_drop.append(index)
            else:
                consecutive_count = 0
            if consecutive_count >= consecutive_threshold:
                break
        otm_final = otm_sorted.drop(to_drop)
        return otm_final

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

    def find_missing_expiries(options_df, futures_df):
        options_expiries = options_df['expiry'].unique()
        futures_expiries = futures_df['expiry'].unique()
        missing_expiries = sorted(list(set(options_expiries) - set(futures_expiries)))
        return missing_expiries

    @staticmethod

    def interpolate_implied_interest_rates(futures_df, missing_expiries):
        futures_df['expiry_ordinal'] = pd.to_datetime(futures_df['expiry']).apply(lambda x: x.toordinal())
        missing_expiries_ordinal = [pd.to_datetime(date).toordinal() for date in missing_expiries]

        # Prepare interpolation function
        interp_func = interp1d(futures_df['expiry_ordinal'], futures_df['implied_interest_rate'], kind='linear',
                               fill_value='extrapolate')

        # Interpolate rates for missing expiries
        interpolated_rates = interp_func(missing_expiries_ordinal)

        # Create DataFrame for the interpolated rates
        interpolated_rates_df = pd.DataFrame({
            'expiry': missing_expiries,
            'implied_interest_rate': interpolated_rates
        })

        return interpolated_rates_df

