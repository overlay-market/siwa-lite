import pymongo
from matplotlib import pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates
import numpy as np  # Importing numpy to help with calculating the SMA

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["siwa_lite"]
collection = db["ordi"]

# Query the collection for documents
documents = collection.find()

# Prepare lists to hold the X and Y values
x_values = []
y_values = []

for doc in documents:
    for detail in doc.get("detail", []):  # Access the 'detail' field if it exists
        if detail["type"] == "inscribe-transfer":  # It can be filtered based on types
            blocktime = detail["blocktime"]  # Get the blocktime
            satoshi = detail["satoshi"]  # Get the satoshi value

            # Convert blocktime to a datetime object and append to X values
            x_values.append(datetime.fromtimestamp(blocktime))
            # Append satoshi to Y values
            y_values.append(satoshi)

# Define window size for SMA
window_size = 5

# Calculate moving average using numpy's convolve function
weights = np.ones(window_size) / window_size
sma_values = np.convolve(y_values, weights, mode="valid")

# Trim x_values to match the length of sma_values (since the convolution reduces the length)
sma_x_values = x_values[window_size - 1 :]

# Create the plot
plt.figure(figsize=(16, 8))

# Plot original Satoshi values
plt.plot_date(x_values, y_values, linestyle="solid", label="Original")

# Plot SMA Satoshi values
plt.plot_date(sma_x_values, sma_values, linestyle="solid", color="red", label="SMA")

# Format the plot
plt.gcf().autofmt_xdate()  # Format the date on the x-axis
date_format = mdates.DateFormatter("%Y-%m-%d %H:%M:%S")
plt.gca().xaxis.set_major_formatter(date_format)

# Add titles, labels, and legend
plt.title("Satoshi Value Over Time with SMA")
plt.xlabel("DateTime")
plt.ylabel("Satoshi")
plt.legend()

# Show the plot
plt.show()
