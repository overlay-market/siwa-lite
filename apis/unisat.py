# from pprint import pprint
import os
from dotenv import load_dotenv
import requests
import json

# import json
# from pathlib import Path

# Adapted from: 
# https://docs.unisat.io/dev/unisat-developer-service/
# https://open-api.unisat.io/swagger.html

# Load environment variables from .env file
load_dotenv()
class UnisatAPI:
    
    def __init__(self):
        api_key = os.environ.get('UNISAT_API_KEY')
        if api_key is None:
            raise ValueError('UNISAT_API_KEY environment variable is not set')
        self.base_url = 'https://open-api.unisat.io/v1/indexer/'
        self.headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }
        self.api_key = api_key

    def _make_request(self, endpoint, params=None):
        url = self.base_url + endpoint
        response = requests.get(url, headers=self.headers, params=params)
        return response
    
    def get_best_block_height(self):
        return self._make_request('brc20/bestheight')

    def get_blockchain_info(self):
        return self._make_request('blockchain/info')

    def get_block_txs(self, height):
        return self._make_request(f'block/{height}/txs')

    def get_tx_info(self, txid):
        return self._make_request(f'tx/{txid}')

    def get_inscription_utxo(self, address):
        return self._make_request(f'address/{address}/inscription-utxo-data')

    def get_inscription_info(self, inscriptionId):
        return self._make_request(f'inscription/info/{inscriptionId}')

    def get_brc20_list(self, start=0, limit=100):
        return self._make_request(f'brc20/list', {'start': start, 'limit': limit})

    def get_brc20_status(self, start=0, limit=10, sort='holders', complete='yes'):
        '''
        sort by (holders/deploy/minted/transactions)
        filter by (yes/no) 
        '''
        return self._make_request(f'brc20/status', {'start': start, 'limit': limit, 'sort': sort, 'complete': complete})

    def get_brc20_ticker_info(self, ticker):
        return self._make_request(f'brc20/{ticker}/info')

    def get_brc20_holders(self, ticker):
        return self._make_request(f'brc20/{ticker}/holders')

    def get_brc20_ticker_history(self, ticker, height, type, start=0, limit=100):
        '''
            type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
        '''
        return self._make_request(f'brc20/{ticker}/history', {'type': type, 'start': start, 'height': height, 'limit': limit})

    def get_history_by_height(self, height, start=0, limit=100):
        return self._make_request(f'brc20/history-by-height/{height}', {'start': start, 'limit': limit})

    def get_brc20_tx_history(self, ticker, txid, type, start=0, limit=100):
        return self._make_request(f'brc20/{ticker}/tx/{txid}/history', {'type': type, 'start': start, 'limit': limit})

    def get_address_brc20_summary(self, address, start=0, limit=100):
        return self._make_request(f'address/{address}/brc20/summary', {'start': start, 'limit': limit})

    def get_address_brc20_summary_by_height(self, address, height, start=0, limit=100):
        return self._make_request(f'address/{address}/brc20/summary-by-height/{height}', {'start': start, 'limit': limit})

    def get_address_brc20_ticker_info(self, address, ticker):
        return self._make_request(f'address/{address}/brc20/{ticker}/info')

    def get_address_brc20_history(self, address, start=0, limit=100):
        return self._make_request(f'address/{address}/brc20/history', {'start': start, 'limit': limit})

    def get_address_brc20_history_by_ticker(self, address, ticker, type, start=0, limit=100):
        '''
        type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
        '''
        return self._make_request(f'address/{address}/brc20/{ticker}/history', {'type': type, 'start': start, 'limit': limit})

    def get_collection_stats(self, collectionId):
        return requests.post(url='https://open-api.unisat.io/v3/market/collection/auction/collection_statistic', headers=self.headers, json={'collectionId': collectionId})
    
def main():
    unisat_api = UnisatAPI()
    # print(unisat_api.get_best_block_height().json())
    # response = unisat_api.get_brc20_ticker_history("ordi", 826827, "inscribe-transfer")
    # print(response.json()["data"])
    # parent_directory = os.path.dirname(os.path.abspath(__file__))
    # json_directory = os.path.join(parent_directory, 'json')
    # os.makedirs(json_directory, exist_ok=True)
    # json_file_path = os.path.join(json_directory, 'get_brc20_tx_history.json')
    # with open(json_file_path, 'w') as file:
    #     json.dump(response.json()["data"], file, indent=4)
    response = unisat_api.get_collection_stats('nodemonkes')
    print("Nodemonkes: ", response.json()["data"]['floorPrice'])
    response = unisat_api.get_collection_stats('bitcoin-frogs')
    print("Bitcoin Frogs: ", response.json()["data"]['floorPrice'])


if __name__ == "__main__":
    main()