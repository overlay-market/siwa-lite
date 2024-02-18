from feeds.data_feed import DataFeed
from feeds.ordinals import OrdinalsFeed

class QuantumCats(OrdinalsFeed):
    ID = 998
    NAME = 'quantum_cats'
    MAGIC_EDEN_COLLECTION_ID = NAME
    OKX_COLLECTION_ID = NAME.replace('_', '-') 
    
# def main():
#     BitcoinFrogs.process_source_data_into_siwa_datapoint()

# if __name__ == "__main__":
#     main()