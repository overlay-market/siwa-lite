import pymongo
from matplotlib import pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["siwa_lite"]
collection = db["ordi"]

# Query the collection for documents
# documents = collection.find({"detail": {"$ne": []}})
documents = collection.find()

# Prepare lists to hold the X and Y values
x_values = []
y_values = []

for doc in documents:
    for detail in doc.get('detail', []):  # Access the 'detail' field if it exists
        if detail['type'] == 'inscribe-deploy':  # It can be filtered based on types
            blocktime = detail['blocktime']  # Get the blocktime
            satoshi = detail['satoshi']  # Get the satoshi value

            # Convert blocktime to a datetime object and append to X values
            x_values.append(datetime.fromtimestamp(blocktime))

            # Append satoshi to Y values
            y_values.append(satoshi)

# Create the plot
plt.figure(figsize=(10, 5))
plt.plot_date(x_values, y_values, linestyle='solid')

# Format the plot
plt.gcf().autofmt_xdate()  # Format the date on the x-axis
date_format = mdates.DateFormatter('%Y-%m-%d %H:%M:%S')
plt.gca().xaxis.set_major_formatter(date_format)

# Add titles and labels
plt.title('Satoshi Value Over Time')
plt.xlabel('DateTime')
plt.ylabel('Satoshi')

# Show the plot
plt.show()