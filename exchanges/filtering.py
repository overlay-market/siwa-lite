class Filtering:
    def __init__(self, options_data):
        self.options_data = options_data
        # self.F = F
        # self.RANGE_MULT = RANGE_MULT
        # self.minimum_bid_threshold = minimum_bid_threshold
        self.Fimp = None
        self.K_ATM = None

    def calculate_Fimp(self, call_data, put_data):
        """
        Calculate the implied forward price based on call and put option data.

        Parameters:
        call_data (list): List of call option data.
        put_data (list): List of put option data.

        Returns:
        float: Implied forward price.
        """
        if not call_data or not put_data:
            return 0

        implied_forward_price = None

        for call_option in call_data:
            call_price = call_option.get("mid_price")
            if call_price is None:
                continue

            min_price_diff = float("inf")
            selected_put_option = None

            for put_option in put_data:
                put_price = put_option.get("mid_price")
                if put_price is None:
                    continue

                price_diff = abs(call_price - put_price)
                if price_diff < min_price_diff:
                    min_price_diff = price_diff
                    selected_put_option = put_option

            if selected_put_option is not None:
                strike_price = self.find_min_difference_strike(call_data, put_data)
                forward_price = self.calculate_forward_price(call_option, put_option)
                implied_forward_price = strike_price + forward_price * (
                    call_price - put_price
                )
                break

        return implied_forward_price if implied_forward_price is not None else 0

    def extract_strike_price(self, option_symbol):
        """
        Extract the strike price from the option symbol.

        Parameters:
        option_symbol (str): Symbol representing the option.

        Returns:
        float: Strike price.
        """
        symbol_parts = option_symbol.split("-")
        if len(symbol_parts) < 3:
            return 0

        strike_price_str = symbol_parts[-2]

        try:
            strike_price = float(strike_price_str)
            return strike_price
        except ValueError:
            return 0

    def calculate_forward_price(self, call_option, put_option):
        """
        Calculate the forward price based on call and put option data.

        Parameters:
        call_option (dict): Call option data.
        put_option (dict): Put option data.

        Returns:
        float: Forward price.
        """
        call_price = call_option.get("mark_price")
        put_price = put_option.get("mark_price")

        if call_price is None or put_price is None:
            return 0

        average_mark_price = (call_price + put_price) / 2
        return average_mark_price

    def find_min_difference_strike(self, call_data, put_data):
        """
        Find the strike price with the minimum difference in mid prices between call and put options.

        Parameters:
        call_data (list): List of call option data.
        put_data (list): List of put option data.

        Returns:
        float: Strike price with the minimum price difference.
        """
        min_diff_strike = None
        min_price_diff = float("inf")

        for call_option in call_data:
            call_price = call_option.get("mid_price")
            if call_price is None:
                continue

            for put_option in put_data:
                put_price = put_option.get("mid_price")
                if put_price is None:
                    continue

                price_diff = abs(call_price - put_price)
                if price_diff < min_price_diff:
                    min_price_diff = price_diff
                    min_diff_strike = self.extract_strike_price(
                        call_option.get("symbol")
                    )

        return min_diff_strike

    def set_ATM_strike(self, call_data, put_data):
        """
        Find the strike price with the minimum difference in mid prices between call and put options.

        Parameters:
        call_data (list): List of call option data.
        put_data (list): List of put option data.

        Returns:
        float: Strike price with the minimum price difference.
        """
        # Extract strike prices from options_data
        Fimp = self.calculate_Fimp(call_data, put_data)

        # Find the strikes less than Fimp
        option_strikes = [
            self.extract_strike_price(option["symbol"]) for option in self.options_data
        ]
        strikes_less_than_Fimp = [strike for strike in option_strikes if strike < Fimp]
        print(strikes_less_than_Fimp)

        max_strike = self.K_ATM = max(strikes_less_than_Fimp)
        return max_strike

    def select_OTM_options(self):
        # Filter OTM options based on K_ATM
        otm_options = [
            option
            for option in self.options_data
            if (option["type"] == "call" and option["strike"] > self.K_ATM)
            or (option["type"] == "put" and option["strike"] < self.K_ATM)
        ]
        return otm_options

    def filter_options_by_strike_range(self):
        # Calculate Kmin and Kmax
        Kmin = self.Fimp / self.RANGE_MULT
        Kmax = self.Fimp * self.RANGE_MULT

        # Filter options within the strike range
        filtered_options = [
            option for option in self.options_data if Kmin < option["strike"] < Kmax
        ]
        return filtered_options

    def filter_options_by_bid_price(self, options):
        # Eliminate options after observing five consecutive bid prices below the threshold
        filtered_options = []
        consecutive_below_threshold = 0

        for option in sorted(options, key=lambda x: x["strike"]):
            if option["bid_price"] > self.minimum_bid_threshold:
                filtered_options.append(option)
                consecutive_below_threshold = 0
            else:
                consecutive_below_threshold += 1
                if consecutive_below_threshold < 5:
                    filtered_options.append(option)
                else:
                    break  # Stop adding options once we hit five below the threshold

        return filtered_options

    def execute(self):
        self.calculate_Fimp()
        self.set_ATM_strike()
        otm_options = self.select_OTM_options()
        strike_filtered_options = self.filter_options_by_strike_range()
        final_filtered_options = self.filter_options_by_bid_price(
            strike_filtered_options
        )
        return final_filtered_options
