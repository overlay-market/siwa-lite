from feeds.data_feed import DataFeed
from collections import deque 
from dataclasses import dataclass
import constants as c
from numpy import random
import pymongo
from apis.unisat import UnisatAPI  # Import required libraries.


class Brc20Feed(DataFeed):
    NAME = 'brc20'
    ID = 0
    HEARTBEAT = 1
    DATAPOINT_DEQUE = deque([], maxlen=100)

    @classmethod
    def connection_db(cls, ticker):
        # Establish a connection to the MongoDB database.
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["siwa_lite"]  # Select the 'siwa_lite' database.
        collection = db[ticker]  # Select the collection based on the ticker name.
        return collection
    
    @classmethod
    def create_new_data_point(cls):
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


        # Connect to the database for the first BRC20 token in the list.
        collection = cls.connection_db(brc20_list[0])

        # Retrieve the last document from the collection, sorted by the _id field (descending).
        last_document = collection.find_one({}, sort=[('_id', pymongo.DESCENDING)])

        # Determine the block height from which to start processing records.
        start_block_height = brc20_ticker_info.json()["data"]["deployHeight"]
        # start_block_height = 805002
        # If the last document's height is greater than the deploy height, update the start block height.
        if last_document["height"] > brc20_ticker_info.json()["data"]["deployHeight"]:
            start_block_height = last_document["height"]
            # Remove the last document which is potentially partial or incomplete.
            collection.delete_one({"_id": last_document["_id"]})

        # Loop through block heights starting from the determined start block height up to the best block height.
        for height in range(start_block_height, best_block_height - 1):
            print("Block height: ", height)  # Output the current block height being processed.
            # Process each event type.
            for event_type in event_types:
                # Query the BRC20 ticker history for the current block height and event type.
                respond = unisat_api.get_brc20_ticker_history(brc20_list[0], height, event_type, 0, 100).json()["data"]
                # If the response is not empty, insert the data into the MongoDB collection.
                if not respond or not respond['detail']:
                    continue
                return respond
