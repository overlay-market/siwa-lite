import pymongo
from apis.unisat import UnisatAPI
import time

event_types = [
    "inscribe-deploy",
    "inscribe-mint",
    "inscribe-transfer",
    "transfer",
    "send",
    "receive",
]

unisat_api = UnisatAPI()

best_block_height = unisat_api.get_best_block_height().json()["data"]["height"]
brc20_list = unisat_api.get_brc20_list(0, 300).json()["data"]["detail"]
brc20_ticker_info = unisat_api.get_brc20_ticker_info(brc20_list[0])
# last_tx = unisat_api.get_brc20_ticker_history(brc20_list[0], best_block_height, event_type, 0, 100).json()["data"]

def connection_db(ticker):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["siwa_lite"]
    collection = db[ticker]
    return collection

def store_db():
    collection = connection_db(brc20_list[0])
    for height in range(brc20_ticker_info.json()["data"]["deployHeight"], best_block_height):
        print("Block height: ", height)
        for event_type in event_types:
            respond = unisat_api.get_brc20_ticker_history(brc20_list[0], height, event_type, 0, 100).json()["data"]
            collection.insert_one(respond)

store_db()
