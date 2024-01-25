import hashlib
import time

import requests

from exchanges.exchange_manager import ExchangeManager


class ByBitManager(ExchangeManager):
    def __init__(self, api_key, api_secret, testnet=False, timeout=10):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.timeout = timeout
        self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.session.headers.update({"Accept": "application/json"})
        self.session.headers.update({"User-Agent": "python/bybit"})

    def _request(self, endpoint, method="GET", params=None, data=None):
        url = self.base_url + endpoint
        if params is None:
            params = {}
        if data is None:
            data = {}
        params["api_key"] = self.api_key
        params["timestamp"] = int(time.time() * 1000)
        params["recv_window"] = 5000
        params["sign"] = self._sign(params)
        response = self.session.request(method, url, params=params, json=data, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _sign(self, params):
        query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        return hashlib.sha256((query_string + self.api_secret).encode("utf-8")).hexdigest()

    def get_account(self):
        return self._request("GET", "/v2/private/wallet/balance")

    def get_order(self, order_id):
        return self._request("GET", "/v2/private/order", params={"order_id": order_id})

    def get_orders(self, symbol, order_link_id=None, order=None, page=None, limit=None, order_status=None):
        params = {"symbol": symbol}
        if order_link_id is not None:
            params["order_link_id"] = order_link_id
        if order is not None:
            params["order"] = order
        if page is not None:
            params["page"] = page
        if limit is not None:
            params["limit"] = limit
        if order_status is not None:
            params["order_status"] = order_status
        return self._request("GET", "/v2/private/order/list", params=params)

    def get_options(self, symbol, page=None, limit=None):
        params = {"symbol": symbol}
        if page is not None:
            params["page"] = page
        if limit is not None:
            params["limit"] = limit
        return self._request("GET", "/v2/private/option/list", params=params)