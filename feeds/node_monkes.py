from feeds.data_feed import DataFeed
from feeds.ordinals import OrdinalsFeed

class NodeMonkes(OrdinalsFeed):
    ID = 0
    NAME = 'node_monkes'
    MAGIC_EDEN_COLLECTION_ID = 'nodemonkes'
    OKX_COLLECTION_ID = 'nodemonkes-3'


# def main():
#     NodeMonkes.process_source_data_into_siwa_datapoint()

# if __name__ == "__main__":
#     main()