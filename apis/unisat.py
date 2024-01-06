# from pprint import pprint
import os
from dotenv import load_dotenv
import requests
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

    def get_blockchain_info(self):
        return self._make_request('blockchain/info')

    def get_block_txs(self, height):
        return self._make_request(f'block/{height}/txs')

    def get_tx_info(self, txid):
        return self._make_request(f'tx/{txid}')

    def get_inscription_utxo(self, address):
        return self._make_request(f'address/{address}/inscription-utxo-data')

    def get_inscription_info(self, inscriptionid):
        return self._make_request(f'inscription/info/{inscriptionid}')

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

    def get_brc20_ticker_history(self, ticker, txid, type, start=0, limit=100):
        '''
            type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
        '''
        return self._make_request(f'brc20/{ticker}/tx/{txid}', {'type': type, 'start': start, 'limit': limit})

    def get_history_by_height(self, height, start=0, limit=100):
        return self._make_request(f'brc20/history-by-height/{height}', {'start': start, 'limit': limit})

    def get_brc20_tx_history(self, ticker, txid, start=0, limit=100):
        return self._make_request(f'brc20/{ticker}/tx/{txid}/history', {'start': start, 'limit': limit})

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

    def get_transferable_inscriptions(self, address, ticker):
        return self._make_request(f'address/{address}/brc20/{ticker}/transferable-inscriptions')
    
def main():
    unisat_api = UnisatAPI()
    response = unisat_api.get_brc20_ticker_history("EFIL", "45a76470f80982d769b1974181cd4f7261084ac8db3dcb1cd4547f9fe91590cf", "inscribe-deploy", 0, 100)
    print(response.json()["data"].keys())

if __name__ == "__main__":
    main()