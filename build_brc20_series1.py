import pymongo
from apis.unisat import UnisatAPI

# import threading
import time

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["siwa_lite"]
collection = db["brc20_txs"]

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

tickers = {}
query = {"ticker": brc20_list[0]}
sort_field = "_id"
# collection.insert_one(
#     {"ticker": brc20_list[0]}
# )

def store_db():
    global best_block_height
    global tickers
    tickers[brc20_list[0]] = collection.find_one(query)
    print(best_block_height)
    _best_block_height = unisat_api.get_best_block_height().json()["data"]["height"]
    if _best_block_height > best_block_height:
        print("first")
        best_block_height = _best_block_height
        tx_list = []
        for event_type in event_types:
            tx_item = unisat_api.get_brc20_ticker_history(
                brc20_list[0], _best_block_height, event_type, 0, 100
            ).json()["data"]
            tx_list = tx_list + tx_item
        origin_history = tickers[brc20_list[0]]["history"]
        updated_history = origin_history.append(tx_list)
        collection.update_one(
            query,
            {
                "$set": {
                    "history": {
                        "block_height": _best_block_height,
                        "details": updated_history
                    }
                }
            },
        )
    else:
        print("second")
        total_trc_on_event_type = 0
        for event_type in event_types:
            tx_list = []
            tx_item = unisat_api.get_brc20_ticker_history(
                brc20_list[0], _best_block_height, event_type, 0, 100
            ).json()["data"]
            print(tx_item)
            if (tickers[brc20_list[0]] is None):
                print("second-1")
                collection.insert_one({
                    "ticker": brc20_list[0],
                    "history": {
                        "block_height": _best_block_height,
                        "details": [tx_item]
                    }
                })
                total_trc_on_event_type = tx_item["total"]
                return
            elif (
                tx_item["total"] != total_trc_on_event_type
            ):
                print("second-2")
                tx_list = tx_list.append(tx_item)
                collection.update_one(
                    query,
                    {
                        "$set": {
                            "history": {
                                "block_height": _best_block_height,
                                "details": tx_list
                            }
                        }
                    },
                )
            total_trc_on_event_type = tx_item["total"]

def run_interval():
    while True:
        store_db()
        time.sleep(5)  # 300 seconds = 5 minutes


run_interval()
