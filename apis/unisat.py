from pprint import pprint
import requests
import json
from pathlib import Path

# Adapted from: 
# https://docs.unisat.io/dev/unisat-developer-service/
# https://open-api.unisat.io/swagger.html

# Headers
headers = {
    'accept': 'application/json',
    'Authorization': 'Bearer 593b09946ab4c0749af07064803c7868c179e86162bf94c4b23a2b157f67c967' 
}

URL = 'https://open-api.unisat.io/v1/indexer/'

def get_blockchain_info():
    url = URL + 'blockchain/info'
    response = requests.get(url, headers=headers)
    return response

def get_block_txs(height):
    url = URL + f'block/{height}/txs'
    response = requests.get(url, headers=headers)
    return response

def get_tx_info(txid):
    url = URL + f'tx/{txid}'
    response = requests.get(url, headers=headers)
    return response 

def get_inscription_utxo(address):
    url = URL + f'address/{address}/inscription-utxo-data'
    response = requests.get(url, headers=headers)
    return response 

def get_inscription_info(inscriptionid):
    url = URL + f'inscription/info/{inscriptionid}'
    response = requests.get(url, headers=headers)
    return response 

def get_brc20_list(start=0, limit=100):
    url = URL + 'brc20/list' + f'?start={start}&limit={limit}'
    response = requests.get(url, headers=headers)
    return response

def get_brc20_status(start=0, limit=10, sort='holders', complete='yes'):
    '''
    sort by (holders/deploy/minted/transactions)
    filter by (yes/no) 
    '''
    url = URL + 'brc20/status' + f'?start={start}&limit={limit}&sort={sort}&complete={complete}'
    response = requests.get(url, headers=headers)
    return response

def get_brc20_ticker_info(ticker):
    url = URL + f'brc20/{ticker}/info'
    response = requests.get(url, headers=headers)
    return response

def get_brc20_holders(ticker):
    url = URL + f'brc20/{ticker}/holders'
    response = requests.get(url, headers=headers)
    return response

def get_brc20_ticker_history(ticker, txid, type, start=0, limit=100):
    '''
        type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
    '''
    url = URL + f'brc20/{ticker}/tx/{txid}' + f'?type={type}&start={start}&limit={limit}'
    response = requests.get(url, headers=headers)
    return response

def get_history_by_height(height, start=0, limit=100):
    url = URL + f'brc20/history-by-height/{height}' + f'?start={start}&limit={limit}'
    response = requests.get(url, headers=headers)
    return response

def get_brc20_tx_history(ticker, txid, start=0, limit=100):
    url = URL + f'brc20/{ticker}/tx/{txid}/history' + f'?start={start}&limit={limit}'
    response = requests.get(url, headers=headers)
    return response

def get_address_brc20_summary(address, start=0, limit=100):
    url = URL + f'address/{address}/brc20/summary' + f'?start={start}&limit={limit}'    
    response = requests.get(url, headers=headers)
    return response

def get_address_brc20_summary_by_height(address, height, start=0, limit=100):
    url = URL + f'address/{address}/brc20/summary-by-height/{height}' + f'?start={start}&limit={limit}'    
    response = requests.get(url, headers=headers)
    return response

def get_address_brc20_ticker_info(address, ticker):
    url = URL + f'address/{address}/brc20/{ticker}/info'
    response = requests.get(url, headers=headers)
    return response

def get_address_brc20_history(address, start=0, limit=100):
    url = URL + f'address/{address}/brc20/history' + f'?start={start}&limit={limit}'
    response = requests.get(url, headers=headers)
    return response 

def get_address_brc20_history_by_ticker(address, ticker, type, start=0, limit=100):
    '''
    type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
    '''
    url = URL + f'address/{address}/brc20/{ticker}/history' + f'?type={type}&start={start}&limit={limit}'
    response = requests.get(url, headers=headers)
    return response

def get_transferable_inscriptions(address, ticker):
    url = URL + f'address/{address}/brc20/{ticker}/transferable-inscriptions'
    response = requests.get(url, headers=headers)   
    return response