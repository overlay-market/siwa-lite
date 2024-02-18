# from pprint import pprint
import os
from dotenv import load_dotenv
import requests
import json

class MagicEdenAPI:
    
    def __init__(self):
        api_key = os.environ.get('MAGIC_EDEN_API_KEY')
        if api_key is None:
            raise ValueError('MAGIC_EDEN_API_KEY environment variable is not set')
        self.base_url = "https://api-mainnet.magiceden.dev/v2/ord/"
        self.headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }
        self.api_key = api_key

    
    def _make_request(self, endpoint, params=None):
        url = self.base_url + endpoint
        response = requests.get(url, headers=self.headers, params=params)
        return response
    
    def get_popular_collections(self, window, limit: int):
        """
        window allowed values: 1h, 6h, 1d, 7d, 30d
        limit = should be a multiple of 12
        """
        return self._make_request(f'/btc/popular_collections?window={window}&limit={limit}')
     
    def get_collection(self, collection_symbol):
        return self._make_request(f'/btc/stat?collectionSymbol={collection_symbol}')
    
    def get_floor_price(self, collection_symbol):
        return float(self.get_collection(collection_symbol).json()['floorPrice'])/1e8 # convert from sats to btc
    
def main():
    me = MagicEdenAPI()
    #response = me.get_popular_collections('1d', 12)
    #print(response.json())
    # response = me.get_collection('nodemonkes')
    # print(response.json())
    print(me.get_floor_price('nodemonkes'))

if __name__ == "__main__":
    main()  