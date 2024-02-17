from feeds.data_feed import DataFeed
from feeds.ordinals import OrdinalsFeed

class NodeMonkes(OrdinalsFeed):
    NAME = 'node_monkes'
    COLLECTION_ID = 'nodemonkes'


# def main():
#     NodeMonkes.process_source_data_into_siwa_datapoint()

# if __name__ == "__main__":
#     main()