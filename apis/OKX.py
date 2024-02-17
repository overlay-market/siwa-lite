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


class OKX:
    
    def __init__(self):
        api_key = os.environ.get('OKX_API_KEY')
        secret_key = os.environ.get('OKX_SECRET_KEY')
        passphrase = os.environ.get('OKX_ACCESS_PASSPHRASE')
        print(passphrase)
        if api_key is None or secret_key is None or passphrase is None:
            raise ValueError(f'Problem with keys, environment OKX_API_KEY is {api_key}, \
                             OKX_SECRET_KEY is {secret_key}, OKX_PASSPHRASE is {passphrase}')
      
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

        self.base_url = "https://api-mainnet.magiceden.dev/v2/ord/"
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
            #'OK-ACCESS-PROJECT': api_config['project'] # Only for WaaS APIs
        }

        # Send a GET request using the http.client library
        conn = http.client.HTTPSConnection("www.okx.com")
        params_encoded = urlencode(params, quote_via=quote_plus) if params else None
        conn.request("GET", request_path + f'?{params_encoded}' if params_encoded else request_path, headers=headers)

        # Receive the response
        response = conn.getresponse()
        data = response.read()

        return data.decode("utf-8")

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
    # GET request example
    request_path = '/api/v5/dex/aggregator/quote'
    params = {'chainId': 42161, 
            'amount': 1000000000000, 
            'toTokenAddress': '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8', 
            'fromTokenAddress': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'}
    req = OKX().send_get_request(request_path, params)
    req
    print(req)


if __name__ == "__main__":
    main()  