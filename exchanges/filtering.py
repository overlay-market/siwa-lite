class Filtering:
    def __init__(self, options_data, F, RANGE_MULT, minimum_bid_threshold):
        self.options_data = options_data
        self.F = F
        self.RANGE_MULT = RANGE_MULT
        self.minimum_bid_threshold = minimum_bid_threshold
        self.Fimp = None
        self.K_ATM = None

    def calculate_Fimp(self):
        # Assuming F is provided, this simplifies the calculation.
        # In a real scenario, you'd calculate Fimp based on option prices.
        self.Fimp = self.F  # Placeholder, replace with actual calculation if needed.

    def set_ATM_strike(self):
        # Find the strike closest to Fimp
        closest_strike = min(self.options_data, key=lambda x: abs(x['strike'] - self.Fimp))
        self.K_ATM = closest_strike['strike']

    def select_OTM_options(self):
        # Filter OTM options based on K_ATM
        otm_options = [option for option in self.options_data if (option['type'] == 'call' and option['strike'] > self.K_ATM) or (option['type'] == 'put' and option['strike'] < self.K_ATM)]
        return otm_options

    def filter_options_by_strike_range(self):
        # Calculate Kmin and Kmax
        Kmin = self.Fimp / self.RANGE_MULT
        Kmax = self.Fimp * self.RANGE_MULT

        # Filter options within the strike range
        filtered_options = [option for option in self.options_data if Kmin < option['strike'] < Kmax]
        return filtered_options

    def filter_options_by_bid_price(self, options):
        # Eliminate options after observing five consecutive bid prices below the threshold
        filtered_options = []
        consecutive_below_threshold = 0

        for option in sorted(options, key=lambda x: x['strike']):
            if option['bid_price'] > self.minimum_bid_threshold:
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
        final_filtered_options = self.filter_options_by_bid_price(strike_filtered_options)
        return final_filtered_options
