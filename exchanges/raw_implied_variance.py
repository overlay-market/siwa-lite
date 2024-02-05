import numpy as np


class RawImpliedVariance:
    def calculate_implied_variance(
        self, F_i, K_i_ATM, strikes, option_prices, r_i, T_i, delta_K
    ):
        # Precompute constant
        discount_factor = np.exp(r_i * T_i)

        # Explicitly convert delta_K elements to float
        delta_K = np.array(delta_K, dtype=float)

        # Ensure that all arrays have the same length
        min_length = min(len(strikes), len(option_prices), len(delta_K))
        strikes = np.array(strikes[:min_length], dtype=float)
        option_prices = np.array(option_prices[:min_length], dtype=float)

        # Log-linear extrapolation
        Kmin, Kmax = min(strikes), max(strikes)
        extrapolated_strikes = np.logspace(np.log10(Kmin), np.log10(Kmax), num=1000)
        extrapolated_option_prices = np.interp(
            extrapolated_strikes, strikes, option_prices
        )

        # Log-linear piece-wise interpolation
        interpolated_strikes = np.logspace(np.log10(Kmin), np.log10(Kmax), num=1000)
        interpolated_option_prices = np.interp(
            interpolated_strikes, strikes, option_prices
        )

        # Update strikes and option prices
        strikes = np.concatenate([strikes, extrapolated_strikes, interpolated_strikes])
        option_prices = np.concatenate(
            [option_prices, extrapolated_option_prices, interpolated_option_prices]
        )

        # Reshape arrays to have the same shape for broadcasting
        discount_factor = discount_factor.reshape((1,))
        strikes = strikes.reshape((len(strikes), 1))
        option_prices = option_prices.reshape((len(option_prices), 1))

        # Vectorized operations with broadcasting
        weights = discount_factor * (delta_K / strikes**2)
        sum_term = np.sum(weights * option_prices)

        # Calculate the implied variance using the formula
        implied_variance = (1 / T_i) * (2 * sum_term - ((F_i / K_i_ATM) - 1) ** 2)

        return implied_variance

    def interpolate_variance(self, T_NEAR, T_NEXT, T_INDEX=30 / 365):
        """
        Calculate the weights for the near and next term variances based on the given times to maturity.

        Parameters:
        T_NEAR (list): List containing Time to maturity for the near term.
        T_NEXT (list): List containing Time to maturity for the next term.
        T_INDEX (list): List containing Time to maturity for the index.

        Returns:
        tuple: A tuple containing the weights for the near term (omega_NEAR) and the next term (omega_NEXT).
        """
        if len(T_NEAR) != len(T_NEXT) or len(T_NEXT) != len(T_INDEX):
            raise ValueError("Input lists must have the same length")

        omega_NEAR_t = [
            (T_NEXT[i] - T_INDEX[i])
            / (T_NEXT[i] - T_NEAR[i] + 1e-9)
            / (T_INDEX[i] + 1e-9)
            for i in range(len(T_NEAR))
        ]
        omega_NEXT_t = [
            (T_INDEX[i] - T_NEAR[i])
            / (T_NEXT[i] - T_NEAR[i] + 1e-9)
            / (T_NEXT[i] + 1e-9)
            for i in range(len(T_NEAR))
        ]

        return omega_NEAR_t, omega_NEXT_t

    def calculate_raw_implied_variance(
        self, omega_NEAR_t, sigma2_NEAR_t, omega_NEXT_t, sigma2_NEXT_t
    ):
        """
        Calculate the raw value of implied variance at the index maturity.

        Parameters:
        omega_NEAR_t (float): Weight for the near term variance.
        sigma2_NEAR_t (float): Near term variance.
        omega_NEXT_t (float): Weight for the next term variance.
        sigma2_NEXT_t (float): Next term variance.

        Returns:
        float: The raw value of implied variance at the index maturity.
        """
        sigma2_RAW_t = omega_NEAR_t * sigma2_NEAR_t + omega_NEXT_t * sigma2_NEXT_t
        return sigma2_RAW_t

    def calculate_ewma(self, lambda_param, sigma2_SMOOTH_t_minus_1, sigma2_RAW_t):
        """
        Calculate the Exponentially-Weighted Moving Average (EWMA) of raw implied variance.

        Parameters:
        lambda_param (float): The smoothing parameter lambda.
        sigma2_SMOOTH_t_minus_1 (float): The previous value of the smoothed implied variance.
        sigma2_RAW_t (float): The raw implied variance at time t.

        Returns:
        float: The smoothed implied variance at time t.
        """
        sigma2_SMOOTH_t = (
            lambda_param * sigma2_SMOOTH_t_minus_1 + (1 - lambda_param) * sigma2_RAW_t
        )
        return sigma2_SMOOTH_t

    def calculate_ewma_recursive(
        self, lambda_param, tau, sigma2_SMOOTH_previous, sigma2_RAW_history
    ):
        """
        Calculate the Exponentially-Weighted Moving Average (EWMA) of raw implied variance recursively.

        Parameters:
        lambda_param (float): The smoothing parameter lambda.
        tau (int): The number of periods over which the half-life is defined.
        sigma2_SMOOTH_previous (float): The smoothed variance at time t-tau.
        sigma2_RAW_history (list of float): The raw implied variances from time t-tau to t-1.

        Returns:
        float: The smoothed implied variance at time t.
        """
        ewma = lambda_param**tau * sigma2_SMOOTH_previous
        for i in range(tau):
            ewma += (1 - lambda_param) * (lambda_param**i) * sigma2_RAW_history[i]
        return ewma

    def calculate_lambda_with_half_life(self, tau):
        """
        Calculate the smoothing parameter lambda based on the specified half-life tau.

        Parameters:
        tau (float): The half-life of the exponentially-weighted moving average in seconds.

        Returns:
        float: The calculated smoothing parameter lambda.
        """
        lambda_param = np.exp(-np.log(2) / tau)
        return lambda_param

    def calculate_xVIV(self, sigma_smooth_t):
        """
        Calculate the xVIV value based on the given smoothed variance at time t.

        Parameters:
        sigma_smooth_t (float): The smoothed variance at time t.

        Returns:
        float: The calculated xVIV value.
        """
        return 100 * np.sqrt(sigma_smooth_t**2)

    def calculate_katm_strike(self, strikes, option_prices):
        """
        Calculate the ATM strike based on the given strikes and option prices.

        Parameters:
        strikes (list of float): The strikes of the options.
        option_prices (list of float): The prices of the options.

        Returns:
        float: The calculated ATM strike.
        """
        # Find the index of the minimum value in the option prices
        min_index = np.argmin(option_prices)

        # Return the strike at the index
        return strikes[min_index]
