import pymongo
from apis.unisat import UnisatAPI

event_types = [
    # "inscribe-deploy",
    # "inscribe-mint",
    "inscribe-transfer",
    # "transfer",
    # "send",
    # "receive",
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

collection = connection_db(brc20_list[0])

last_document = collection.find_one({}, sort=[('_id', pymongo.DESCENDING)])

start_block_height = brc20_ticker_info.json()["data"]["deployHeight"]
if last_document["height"] > brc20_ticker_info.json()["data"]["deployHeight"]:
    start_block_height = last_document["height"]
    collection.delete_one({"_id": last_document["_id"]})

def store_db():
    global collection
    for height in range(start_block_height, best_block_height - 1):
        print("Block height: ", height)
        for event_type in event_types:
            respond = unisat_api.get_brc20_ticker_history(brc20_list[0], height, event_type, 0, 100).json()["data"]["detail"]
            if respond is not []:
                collection.insert_one(respond)

store_db()
