import pymongo
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

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
