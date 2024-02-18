from feeds.data_feed import DataFeed
from feeds.ordinals import OrdinalsFeed

class BitcoinFrogs(OrdinalsFeed):
    ID = 99
    NAME = 'bitcoin_frogs'
    MAGIC_EDEN_COLLECTION_ID = 'bitcoin-frogs'
    OKX_COLLECTION_ID = 'bitcoin-frogs'

# def main():
#     BitcoinFrogs.process_source_data_into_siwa_datapoint()

# if __name__ == "__main__":
#     main()