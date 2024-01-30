import pymongo
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from time_series.time_series import TimeSeries
import numpy as np

# Connect to MongoDB (replace with actual connection details)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["siwa_lite"]
collection = db["brc20_data"]

# Query the collection for documents (adjust the query as needed)
documents = list(collection.find({}))

# If you received raw dictionary-like objects and need to normalize datetime field:
for doc in documents:
    if isinstance(doc['timestamp'], (int, float)):  # Assuming UNIX timestamp in seconds
        doc['timestamp'] = datetime.fromtimestamp(doc['timestamp'])

# Convert documents to DataFrame
df = pd.DataFrame(documents)

# Convert 'timestamp' to datetime if necessary and set it as index
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')  # Adjust 'unit' as per requirement
df = df.set_index('timestamp')
df.sort_index(inplace=True)

# Calculate simple moving averages (SMA) to represent `twap_60min` and `twap_10min`
window_sizes = {'twap_60min': 60, 'twap_10min': 10}
for name, window in window_sizes.items():
    df[name] = df['value'].rolling(window, min_periods=1).mean()

# Calculate residuals by subtracting the TWAPs from the original values
for col in window_sizes.keys():
    df[f'{col}_residual'] = df['value'] - df[col]


# Limit df to last ~10 days (10*48 rows) to visualise better
df_last60 = df.iloc[-10*48:, :]
ts = TimeSeries(df_last60, 'timestamp', 'eth_burn')
decom = ts.decompose(period=48)
ts.plot_decomposition()

# Check ADF to see if time series is stationary
series, d = ts.check_stationarity(100)

# Check to see if residual after decomposition is stationary
resid = decom.resid.dropna()
resid = pd.DataFrame(resid)
resid['timestamp'] = resid.index
ts_resid = TimeSeries(pd.DataFrame(resid), 'timestamp', 'resid')
series, d = ts_resid.check_stationarity(100)

ts_all = TimeSeries(df, 'timestamp', 'eth_burn')
decom = ts_all.decompose(period=48)
resid = decom.resid.dropna()
resid = pd.DataFrame(resid)
resid.reset_index(inplace=True)

def rolling_mean_variance(df, t, col):
    """
    Calculate rolling mean and variance with data manipulation:
    1. Adjust data mean to 0.
    2. Set max value to the absolute of the min value.

    Args:
    df (pandas.DataFrame): Input dataframe.
    t (int): Rolling window size.
    col (str): Column name on which to perform the operation.

    Returns:
    pandas.DataFrame: DataFrame with rolling mean and variance.
    """

    def manipulate_data(window):
        # Adjust the mean to 0
        window_mean = np.mean(window)
        window -= window_mean

        # Set max value to negative of the min value
        min_val = np.min(window)
        window[window >= abs(min_val)] = abs(min_val)

        # Calculate the mean and variance of the manipulated window
        mean_adjusted = np.mean(window)  # should be close to 0
        var_adjusted = np.var(window)
        
        return mean_adjusted, var_adjusted

    # Store the results in lists
    rolling_means = []
    rolling_vars = []

    # Perform the rolling window calculation
    for start in range(len(df) - t + 1):
        end = start + t
        window = df[col][start:end]
        mean_adjusted, var_adjusted = manipulate_data(window)
        rolling_means.append(mean_adjusted)
        rolling_vars.append(var_adjusted)

    # Construct the resulting DataFrame
    result = pd.DataFrame({
        'rolling_mean': rolling_means,
        'rolling_var': rolling_vars
    }, index=df.index[t-1:])

    return result

res = rolling_mean_variance(resid, 48, 'resid')
resid_mean_var = res.join(resid)



# Plot the residuals
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['twap_60min_residual'], label='60-min TWAP Residuals')
plt.plot(df.index, df['twap_10min_residual'], label='10-min TWAP Residuals')
plt.title("Residuals of BRC20 Time Series Data")
plt.xlabel("Timestamp")
plt.ylabel("Residual value")
plt.legend()
plt.show()

# Storing residuals back to MongoDB

# Now, the df contains original data, the TWAP values, and the residuals.
# You can save the DataFrame back into MongoDB, in a new collection for example:
new_collection = db['brc20_residuals']
# Convert the DataFrame to dict and store it
residuals_dict = df[['twap_60min_residual', 'twap_10min_residual']].to_dict("records")
# If you wish to store them in the database, you can use insert or update
# Depending on your requirement this could be:
# new_collection.insert_many(residuals_dict)
# or to update existing documents with new fields:
for index, row in df.iterrows():
    update = {"$set": {"twap_60min_residual": row['twap_60min_residual'], 
                       "twap_10min_residual": row['twap_10min_residual']}}
    collection.update_one({"_id": row['_id']}, update)
