from feeds.data_feed import DataFeed
from feeds.ordinals import OrdinalsFeed

class BitcoinFrogs(OrdinalsFeed):
    NAME = 'bitcoin_frogs'
    COLLECTION_ID = 'bitcoin-frogs'


# def main():
#     BitcoinFrogs.process_source_data_into_siwa_datapoint()

# if __name__ == "__main__":
#     main()