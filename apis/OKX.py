# from pprint import pprint
import os
#from dotenv import load_dotenv
import requests
import json

import http.client
import hmac
import hashlib
import base64
from datetime import datetime
from urllib.parse import urlencode, quote_plus
import json


class OKXAPI:
    
    COLLECTIONS_ENDPOINT = "/api/v5/mktplace/nft/ordinals/collections"
    INSCRIPTIONS_ENDPOINT = "/api/v5/explorer/brc20/inscriptions-list"
    
    def __init__(self):
        api_key = os.environ.get('OKX_API_KEY')
        secret_key = os.environ.get('OKX_SECRET_KEY')
        passphrase = os.environ.get('OKX_ACCESS_PASSPHRASE')
        if api_key is None or secret_key is None or passphrase is None:
            raise ValueError(f'Problem with keys, environment OKX_API_KEY is {api_key}, \
                             OKX_SECRET_KEY is {secret_key}, OKX_PASSPHRASE is {passphrase}')
      
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.project = 'Siwa' # Only for WaaS APIs

        self.base_url = "www.okx.com"
        self.headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }

    
    def _make_request(self, endpoint, params=None):
        url = self.base_url + endpoint
        response = requests.get(url, headers=self.headers, params=params)
        return response
    
    @staticmethod
    def pre_hash(timestamp, method, request_path, params):
        # Create a pre-signature based on the string and parameters
        query_string = ''
        if method == 'GET' and params:
            query_string = '?' + urlencode(params)
        if method == 'POST' and params:
            query_string = json.dumps(params)
        return timestamp + method + request_path + query_string

    @staticmethod
    def sign(message, secret_key):
        # Sign the pre-signed string using HMAC-SHA256
        mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), hashlib.sha256)
        d = mac.digest()
        return base64.b64encode(d).decode('utf-8') 

    def create_signature(self, method, request_path, params):
        # Obtain a ISO 8601 format timestamp
        timestamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        # Generate signature
        message = self.pre_hash(timestamp, method, request_path, params)
        signature = self.sign(message, self.secret_key)
        
        return signature, timestamp

    def send_get_request(self, request_path, params):
        # Generate signature
        signature, timestamp = self.create_signature("GET", request_path, params)

        # Generate headers
        headers = {
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'OK-ACCESS-PROJECT': self.project # Only for WaaS APIs
        }

        # Send a GET request using the http.client library
        conn = http.client.HTTPSConnection(self.base_url)
        params_encoded = urlencode(params, quote_via=quote_plus) if params else None
        conn.request("GET", request_path + f'?{params_encoded}' if params_encoded else request_path, headers=headers)

        # Receive the response
        response = conn.getresponse()
        data = response.read()

        return data.decode("utf-8")
    
    def get_ordinals_collections(self):
        return self.send_get_request(self.COLLECTIONS_ENDPOINT, params={})
    
    def get_inscriptions_list(self):
        res = self.send_get_request(self.INSCRIPTIONS_ENDPOINT, params={})
        print(res)
         
    # def get_ordinals_collection(self, slug, cursor, limit, is_brc20):
    #     params = {'slug': slug, 'cursor': cursor, 'limit': limit, 'isBrc20': is_brc20}
    #     res = self.send_get_request(self.COLLECTIONS_ENDPOINT, params)
    #     print(res)
    
    def get_ordinals_collection(self, slug,  limit, is_brc20):
        params = {'slug': slug, 'limit': limit, 'isBrc20': is_brc20}
        res = self.send_get_request(self.COLLECTIONS_ENDPOINT, params)
        print(res)

    def get_floor_price(self, slug):
        params = {'slug': slug, 'limit': 1, 'isBrc20': False}
        res = json.loads(self.send_get_request(self.COLLECTIONS_ENDPOINT, params))
        return float(res['data']['data'][0]['floorPrice'])


    # def send_post_request(request_path, params):
    #     # Generate signature
    #     signature, timestamp = create_signature("POST", request_path, params)

    #     # Generate headers
    #     headers = {
    #         'OK-ACCESS-KEY': api_config['api_key'],
    #         'OK-ACCESS-SIGN': signature,
    #         'OK-ACCESS-TIMESTAMP': timestamp,
    #         'OK-ACCESS-PASSPHRASE': api_config['passphrase'],
    #         'OK-ACCESS-PROJECT': api_config['project'], # Only for WaaS APIs
    #         'Content-Type': 'application/json'  # Required for POST requests
    #     }

    #     # Send a POST request using the http.client library
    #     conn = http.client.HTTPSConnection("www.okx.com")
    #     params_encoded = json.dumps(params) if params else ''
    #     conn.request("POST", request_path, body=params_encoded, headers=headers)

    #     # Receive the response
    #     response = conn.getresponse()
    #     data = response.read()

    #     return data.decode("utf-8")


    
def main():
    okx = OKX()
    #okx.get_inscriptions_list()
    #okx.get_ordinals_collection("nodemonkes-3", 1, False)
    res = okx.get_floor_price("nodemonkes-3")
    breakpoint()
 


if __name__ == "__main__":
    main()  