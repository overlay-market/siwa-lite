import pymongo
from apis.unisat import UnisatAPI  # Import required libraries.

# Define a list of event types to be processed.
# Other event types are commented out, indicating they are currently not in use.
event_types = [
    # "inscribe-deploy",
    # "inscribe-mint",
    "inscribe-transfer",
    # "transfer",
    # "send",
    # "receive",
]

unisat_api = UnisatAPI()  # Instantiate the UnisatAPI class.

# Query the best block height from the Unisat API and extract it from the response.
best_block_height = unisat_api.get_best_block_height().json()["data"]["height"]
# Query the list of BRC20 tokens from the Unisat API and extract the token details.
brc20_list = unisat_api.get_brc20_list(0, 300).json()["data"]["detail"]
# Get ticker info for the first BRC20 token in the list.
brc20_ticker_info = unisat_api.get_brc20_ticker_info(brc20_list[0])
# Uncomment this line to retrieve the last transaction history for a specific event type (currently commented out).
# last_tx = unisat_api.get_brc20_ticker_history(brc20_list[0], best_block_height, event_type, 0, 100).json()["data"]


def connection_db(ticker):
    # Establish a connection to the MongoDB database.
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["siwa_lite"]  # Select the 'siwa_lite' database.
    collection = db[ticker]  # Select the collection based on the ticker name.
    return collection


# Connect to the database for the first BRC20 token in the list.
collection = connection_db(brc20_list[0])

# Retrieve the last document from the collection, sorted by the _id field (descending).
last_document = collection.find_one({}, sort=[("_id", pymongo.DESCENDING)])

# Determine the block height from which to start processing records.
start_block_height = brc20_ticker_info.json()["data"]["deployHeight"]
# If the last document's height is greater than the deploy height, update the start block height.
if last_document["height"] > brc20_ticker_info.json()["data"]["deployHeight"]:
    start_block_height = last_document["height"]
    # Remove the last document which is potentially partial or incomplete.
    collection.delete_one({"_id": last_document["_id"]})


def store_db():
    global collection  # Declare the global collection variable to be used within this function.
    # Loop through block heights starting from the determined start block height up to the best block height.
    for height in range(start_block_height, best_block_height - 1):
        print(
            "Block height: ", height
        )  # Output the current block height being processed.
        # Process each event type.
        for event_type in event_types:
            # Query the BRC20 ticker history for the current block height and event type.
            respond = unisat_api.get_brc20_ticker_history(
                brc20_list[0], height, event_type, 0, 100
            ).json()["data"]["detail"]
            # If the response is not empty, insert the data into the MongoDB collection.
            if respond is not []:
                collection.insert_one(respond)


# Call the store_db function to start storing records into the database.
store_db()
