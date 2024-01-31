import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.stattools import adfuller


class TimeSeries:
    def __init__(self, df, time_col, target_col):
        self.df = df
        self.time_col = time_col
        self.target_col = target_col
        # Make time_col the index but also retain it as a column
        self.df.set_index(time_col, inplace=True)
        self.df[time_col] = self.df.index

    def decompose(self, model='additive', period=48):
        """
        Decompose the time series into trend, seasonal, and residual
        components.
        """
        self.decomposition = seasonal_decompose(self.df[self.target_col],
                                                model=model, period=period)

        return self.decomposition

    def plot_decomposition(self):
        # Plot the decomposed components
        fig, axes = plt.subplots(4, 1, sharex=True, figsize=(12, 9))
        self.decomposition.observed.plot(ax=axes[0], legend=False,
                                         title='Observed')
        axes[0].set_ylabel('Observed')

        self.decomposition.trend.plot(ax=axes[1], legend=False,
                                      title='Trend')
        axes[1].set_ylabel('Trend')

        self.decomposition.seasonal.plot(ax=axes[2], legend=False,
                                         title='Seasonal')
        axes[2].set_ylabel('Seasonal')

        self.decomposition.resid.plot(ax=axes[3], legend=False,
                                      title='Residual')
        axes[3].set_ylabel('Residual')

        plt.tight_layout()
        plt.show()

    def detrend(self):
        """Remove the trend from the time series."""
        detrended = self.df[self.target_col] - self.decomposition.trend
        return detrended

    def deseasonalize(self):
        """Remove the seasonality from the time series."""
        detrended = self.df[self.target_col] - self.decomposition.trend
        deseasonalized = detrended - self.decomposition.seasonal
        return deseasonalized

    def reconstruct(self):
        """Reconstruct the time series from the components."""

        reconstructed = self.decomposition.trend\
            + self.decomposition.seasonal\
            + self.decomposition.resid
        return reconstructed

    def plot_autocorrelation(self, lags=50):
        """
        Plot the Autocorrelation function for the time series.
        Args:
            lags: Number of lags to include in the ACF plot.
        """
        plt.figure(figsize=(12, 5))
        plot_acf(self.df[self.target_col], lags=lags)
        plt.title('Autocorrelation Function')
        plt.show()

    def check_stationarity(self, max_lags=1):
        """
        Check if the time series is stationary and difference it if not.
        Args:
            max_lags: Maximum number of lags to try for differencing.
        Returns:
            A tuple with the stationary series and the order of differencing
            applied.
        """
        # Perform ADF test on the original data
        adf_result = adfuller(self.df[self.target_col])
        print(f'ADF Statistic: {adf_result[0]}')
        print(f'p-value: {adf_result[1]}')

        if adf_result[1] <= 0.05:
            print('Series is stationary.')
            return self.df[self.target_col], 0
        else:
            # Series is not stationary: try differencing up to max_lags
            for d in range(1, max_lags + 1):
                differenced_series = self.df[self.target_col].diff(d).dropna()
                adf_result_diff = adfuller(differenced_series)
                print(f'ADF Statistic for difference order {d}: {adf_result_diff[0]}')  # noqa E501
                print(f'p-value for difference order {d}: {adf_result_diff[1]}')  # noqa E501

                if adf_result_diff[1] <= 0.05:
                    print(f'Series is stationary after differencing of order {d}.')  # noqa E501
                    return differenced_series, d

            print(f'Series is still non-stationary after differencing up to order {max_lags}.')  # noqa E501
            return None, None
