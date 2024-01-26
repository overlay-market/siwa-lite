import pymongo
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Connect to MongoDB (replace with actual connection details)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["siwa_lite"]
collection = db["brc20_data"]

# Query the collection for documents
documents = collection.find()

# Prepare dataframe to hold the data
df = pd.DataFrame(columns=['timestamp', 'value'])

# Extract data to dataframe
for doc in documents:
    # Assuming doc contains 'timestamp' and 'value'
    timestamp = doc.get('timestamp')
    value = doc.get('value')
    
    # Convert timestamp to a readable datetime format and append to dataframe
    datetime_object = datetime.fromtimestamp(timestamp)
    df = df.append({'timestamp': datetime_object, 'value': value}, ignore_index=True)

# Sort by timestamp in case the data was not in sequential order
df.sort_values('timestamp', inplace=True)
df.set_index('timestamp', inplace=True)

# Calculate simple moving averages (SMA) to represent `twap_60min` and `twap_10min`
window_sizes = {'twap_60min': 60, 'twap_10min': 10}
for name, window in window_sizes.items():
    df[name] = df['value'].rolling(window, min_periods=1).mean()

# Calculate residuals by subtracting the TWAPs from the original values
for col in window_sizes.keys():
    df[f'{col}_residual'] = df['value'] - df[col]

# Plot the residuals
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['twap_60min_residual'], label='60-min TWAP Residuals')
plt.plot(df.index, df['twap_10min_residual'], label='10-min TWAP Residuals')
plt.title("Residuals of BRC20 Time Series Data")
plt.xlabel("Timestamp")
plt.ylabel("Residual value")
plt.legend()
plt.show()

# Now, the df contains the original data, the TWAP values, and the residuals.
# You can perform further analysis, save the dataframe, or manipulate it as needed.
