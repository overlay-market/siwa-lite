# Import necessary libraries.
import pymongo
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from research.decomposer import Decomposer  # Ensure you have this module or package installed, it's not standard.

class TakingResidual:
    # Initializer / Instance Attributes
    def __init__(self):
        # Connect to a local MongoDB instance and select database 'siwa_lite'.
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = client["siwa_lite"]

    # Method to retrieve and process time-series data from MongoDB.
    def get_time_series_data(self):
        # Access the 'ordi' collection in the database.
        collection = self.db["ordi"]

        # Fetch documents from the 'ordi' collection. Modify the query as needed.
        pre_docs = list(collection.find({}))

        # Initialize a dictionary to store processed data.
        documents = {'timestamp': [], 'fee': []}

        # Loop through the documents and extract needed data.
        for doc in pre_docs:
            for detail in doc.get('detail', []):  # Check if 'detail' key is in the doc
                if detail['type'] == 'inscribe-transfer':  # Filter for a specific type of detail
                    # Parse individual data fields.
                    blocktime = detail['blocktime']
                    fee = detail['fee']
                    satoshi = detail['satoshi']
                    # Append the processed fee calculation to document lists.
                    documents['timestamp'].append(blocktime)
                    documents['fee'].append(fee * satoshi * 1e-8)

        # Convert the dictionary to a pandas DataFrame.
        df = pd.DataFrame(documents)
        # Convert 'timestamp' values to datetime objects.
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

        # Return the DataFrame for further processing.
        return df

    # Method to plot fee data as a time series.
    def plot_fee(self, df):
        plt.figure(figsize=(10, 5))
        plt.plot(df['timestamp'], df['fee'])

        # Configure x-axis to show dates.
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=4))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        # Auto-formatting for date labels on the x-axis.
        plt.gcf().autofmt_xdate()

        # Add labels and a title to the plot.
        plt.title('Fee Over Time')
        plt.xlabel('Timestamp')
        plt.ylabel('Fee')

        # Optionally, rotate x-axis date labels and adjust layout.
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save the figure to a file.
        plt.savefig('fee.png')

    # Method to get and plot residuals from time series decomposition.
    def get_residual(self, df):
        # Decompose the time series data.
        ts_all = Decomposer(df, 'timestamp', 'fee')
        decom = ts_all.decompose(period=48)
        resid = decom.resid.dropna()
        resid = pd.DataFrame(resid)
        resid.reset_index(inplace=True)
        # Plot the decomposition for visual analysis.
        ts_all.plot_decomposition()

        # Return residual component of the decomposition.
        return resid

    # Method to simulate a GBM path using the residuals from decomposition.
    def get_gbm_path(self, resid):
        gbm_path = [-10]  # Initialize GBM path with starting value of -10.

        # Time step for increments.
        dt = 1

        # Generate the GBM path.
        for index, row in resid.iloc[1:5000].iterrows():  # Iterate over the data.
            # Placeholder values for mean (mu) and volatility (sigma).
            mu = 0.5
            sigma = 1
            S0 = gbm_path[-1]

            # Calculate the next mode point in the GBM path.
            next_point = S0 * np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * row['resid'])
            gbm_path.append(next_point)

        # Return the generated GBM path.
        return gbm_path

    # Method to plot the GBM path.
    def plot_gbm_path(self, gbm_path):
        plt.figure(figsize=(10, 6))
        plt.plot(gbm_path, lw=1)
        plt.title('Geometric Brownian Motion (GBM) Path')
        plt.xlabel('Time Steps')
        plt.ylabel('GBM Value')
        plt.grid(True)
        plt.savefig('final.png')

# Main routine to execute when the script is run.
if __name__ == '__main__':
    tr = TakingResidual()  # Instantiate the class.
    df = tr.get_time_series_data()  # Get and process data.
    tr.plot_fee(df)  # Plot fee time series.
    re = tr.get_residual(df)  # Get residuals from decomposition.
    gp = tr.get_gbm_path(re)  # Generate GBM path.
    tr.plot_gbm_path(gp)  # Plot GBM path.
