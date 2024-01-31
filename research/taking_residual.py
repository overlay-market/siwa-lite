import pymongo
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from decomposer import Decomposer
import numpy as np
import matplotlib.dates as mdates

# Connect to MongoDB (replace with actual connection details)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["siwa_lite"]
collection = db["ordi"]

# Query the collection for documents (adjust the query as needed)
pre_docs = list(collection.find({}))


documents = {'timestamp': [],'fee': []}

# If you received raw dictionary-like objects and need to normalize datetime field:
for doc in pre_docs:
    for detail in doc.get('detail', []):  # Access the 'detail' field if it exists
        if detail['type'] == 'inscribe-transfer':  # It can be filtered based on types
            blocktime = detail['blocktime']  # Get the blocktime
            fee = detail['fee']  # Get the fee value
            satoshi = detail['satoshi']  # Get the fee value
            documents['timestamp'].append(blocktime)
            documents['fee'].append(fee*satoshi*0.00000001)

# Convert documents to DataFrame
df = pd.DataFrame(documents)
# Convert 'timestamp' to datetime if necessary
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')  # Adjust 'unit' as per requirement

plt.figure(figsize=(10, 5))  # Set the figure size as desired
plt.plot(df['timestamp'], df['fee'], marker='o')  # Using 'date' for x-axis, and 'o' as marker for data points

# Set the x-axis major locator to display ticks for each day and the formatter for the dates
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show a tick for every day
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format the date

# Improve the layout of the dates on the x-axis
plt.gcf().autofmt_xdate()  # Automatically adjusts the x-axis labels

# Add some labels and title
plt.title('Fee Over Time')
plt.xlabel('Date')
plt.ylabel('Fee')

# Optionally, configure the x-axis to format the date properly and/or to rotate the date labels for better readability
plt.xticks(rotation=45)  # Rotate the x-axis labels if they overlap
plt.tight_layout()  # Adjust the padding between and around the subplots for better fit
plt.savefig('fee.png')

# # Group by the date and calculate mean fee for each day
# df_daily = df.groupby(df['timestamp'].dt.date).agg({'fee': 'mean'}).reset_index()

# # Optionally, you can rename the columns if you want to reflect that these are daily records
# df_daily.columns = ['date', 'mean_fee']

# # If you still need a 'timestamp' column with time set to 00:00, you can convert the 'date' back to datetime
# df_daily['date'] = pd.to_datetime(df_daily['date'])

# df_last60 = df_daily.iloc[-10*48:, :]
# plt.figure(figsize=(10, 5))  # Set the figure size as desired
# plt.plot(df_last60['date'], df_last60['mean_fee'], marker='o')  # Using 'date' for x-axis, and 'o' as marker for data points

# # Set the x-axis major locator to display ticks for each day and the formatter for the dates
# plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show a tick for every day
# plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format the date

# # Improve the layout of the dates on the x-axis
# plt.gcf().autofmt_xdate()  # Automatically adjusts the x-axis labels

# # Add some labels and title
# plt.title('Daily Mean Fee Over Time')
# plt.xlabel('Date')
# plt.ylabel('Mean Fee')

# # Optionally, configure the x-axis to format the date properly and/or to rotate the date labels for better readability
# plt.xticks(rotation=45)  # Rotate the x-axis labels if they overlap
# plt.tight_layout()  # Adjust the padding between and around the subplots for better fit
# plt.savefig('mean_fee.png')

# Set the date as the index if required
# df_daily = df_daily.set_index('date')

# Limit df to last ~10 days (10*48 rows) to visualise better
# df_last60 = df.iloc[-10*48:, :]
# ts = Decomposer(df_last60, 'timestamp', 'fee')
# decom = ts.decompose(period=48)
# ts.plot_decomposition()

# # Check ADF to see if time series is stationary
# series, d = ts.check_stationarity(100)

# # Check to see if residual after decomposition is stationary
# resid = decom.resid.dropna()
# resid = pd.DataFrame(resid)
# resid['timestamp'] = resid.index
# ts_resid = Decomposer(pd.DataFrame(resid), 'timestamp', 'resid')
# series, d = ts_resid.check_stationarity(100)

ts_all = Decomposer(df, 'timestamp', 'fee')
decom = ts_all.decompose(period=48)
resid = decom.resid.dropna()
resid = pd.DataFrame(resid)
resid.reset_index(inplace=True)
ts_all.plot_decomposition()

# def rolling_mean_variance(df, t, col):
#     """
#     Calculate rolling mean and variance with data manipulation:
#     1. Adjust data mean to 0.
#     2. Set max value to the absolute of the min value.

#     Args:
#     df (pandas.DataFrame): Input dataframe.
#     t (int): Rolling window size.
#     col (str): Column name on which to perform the operation.

#     Returns:
#     pandas.DataFrame: DataFrame with rolling mean and variance.
#     """

#     def manipulate_data(window):
#         # Adjust the mean to 0
#         window_mean = np.mean(window)
#         window -= window_mean

#         # Set max value to negative of the min value
#         min_val = np.min(window)
#         window[window >= abs(min_val)] = abs(min_val)

#         # Calculate the mean and variance of the manipulated window
#         mean_adjusted = np.mean(window)  # should be close to 0
#         var_adjusted = np.var(window)
        
#         return mean_adjusted, var_adjusted

#     # Store the results in lists
#     rolling_means = []
#     rolling_vars = []

#     # Perform the rolling window calculation
#     for start in range(len(df) - t + 1):
#         end = start + t
#         window = df[col][start:end]
#         mean_adjusted, var_adjusted = manipulate_data(window)
#         rolling_means.append(mean_adjusted)
#         rolling_vars.append(var_adjusted)

#     # Construct the resulting DataFrame
#     result = pd.DataFrame({
#         'rolling_mean': rolling_means,
#         'rolling_var': rolling_vars
#     }, index=df.index[t-1:])

#     return result

# res = rolling_mean_variance(resid, 48, 'resid')
# resid_mean_var = res.join(resid)
# resid = resid_mean_var
# def get_delta(point, resid):
#     if resid > .5:
#         resid = .5
#     elif resid < -.5:
#         resid = -.5
#     #res = resid*10
#     res = point * (.03/5)*resid
#     #print(res)
#     return res#point*resid/(100*3)

# print(resid)

# Initialize the GBM path
gbm_path = [resid['resid'].iloc[0]]  # Starting with the first 'resid' value
gbm_path = [-10]  # Starting with 100 (arbitrary choice)

# Time increment
dt = 1

# Iterate through the DataFrame to generate the GBM path
for index, row in resid.iloc[1:5000].iterrows():  # Start from the second row
    mu = 1/2 # row['rolling_var']/2000000  # sigma^2/2
    sigma = 1 # np.sqrt(row['rolling_var'])/1000
    S0 = gbm_path[-1]
    
    # print(mu, sigma)
    # Random component
    # Z = np.random.standard_normal()
    
    # Calculate the next point using the GBM formula
    next_point = S0 * np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * row['resid'])
    gbm_path.append(next_point)

# Plot the GBM path
plt.figure(figsize=(10, 6))
plt.plot(gbm_path, lw=1)
plt.title('Geometric Brownian Motion (GBM) Path')
plt.xlabel('Time Steps')
plt.ylabel('GBM Value')
plt.grid(True)
plt.savefig('final.png')