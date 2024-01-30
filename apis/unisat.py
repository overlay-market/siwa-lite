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

# Headers
headers = {
    "accept": "application/json",
    "Authorization": "Bearer 593b09946ab4c0749af07064803c7868c179e86162bf94c4b23a2b157f67c967",
}

URL = "https://open-api.unisat.io/v1/indexer/"


def get_blockchain_info():
    url = URL + "blockchain/info"
    response = requests.get(url, headers=headers)
    return response


def get_block_txs(height):
    url = URL + f"block/{height}/txs"
    response = requests.get(url, headers=headers)
    return response


def get_tx_info(txid):
    url = URL + f"tx/{txid}"
    response = requests.get(url, headers=headers)
    return response


def get_inscription_utxo(address):
    url = URL + f"address/{address}/inscription-utxo-data"
    response = requests.get(url, headers=headers)
    return response


def get_inscription_info(inscriptionid):
    url = URL + f"inscription/info/{inscriptionid}"
    response = requests.get(url, headers=headers)
    return response


def get_brc20_list(start=0, limit=100):
    url = URL + "brc20/list" + f"?start={start}&limit={limit}"
    response = requests.get(url, headers=headers)
    return response


def get_brc20_status(start=0, limit=10, sort="holders", complete="yes"):
    """
    sort by (holders/deploy/minted/transactions)
    filter by (yes/no)
    """
    url = (
        URL
        + "brc20/status"
        + f"?start={start}&limit={limit}&sort={sort}&complete={complete}"
    )
    response = requests.get(url, headers=headers)
    return response


def get_brc20_ticker_info(ticker):
    url = URL + f"brc20/{ticker}/info"
    response = requests.get(url, headers=headers)
    return response


def get_brc20_holders(ticker):
    url = URL + f"brc20/{ticker}/holders"
    response = requests.get(url, headers=headers)
    return response


def get_brc20_ticker_history(ticker, txid, type, start=0, limit=100):
    """
    type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
    """
    url = (
        URL + f"brc20/{ticker}/tx/{txid}" + f"?type={type}&start={start}&limit={limit}"
    )
    response = requests.get(url, headers=headers)
    return response


def get_history_by_height(height, start=0, limit=100):
    url = URL + f"brc20/history-by-height/{height}" + f"?start={start}&limit={limit}"
    response = requests.get(url, headers=headers)
    return response


def get_brc20_tx_history(ticker, txid, start=0, limit=100):
    url = URL + f"brc20/{ticker}/tx/{txid}/history" + f"?start={start}&limit={limit}"
    response = requests.get(url, headers=headers)
    return response


def get_address_brc20_summary(address, start=0, limit=100):
    url = URL + f"address/{address}/brc20/summary" + f"?start={start}&limit={limit}"
    response = requests.get(url, headers=headers)
    return response


def get_address_brc20_summary_by_height(address, height, start=0, limit=100):
    url = (
        URL
        + f"address/{address}/brc20/summary-by-height/{height}"
        + f"?start={start}&limit={limit}"
    )
    response = requests.get(url, headers=headers)
    return response


def get_address_brc20_ticker_info(address, ticker):
    url = URL + f"address/{address}/brc20/{ticker}/info"
    response = requests.get(url, headers=headers)
    return response


def get_address_brc20_history(address, start=0, limit=100):
    url = URL + f"address/{address}/brc20/history" + f"?start={start}&limit={limit}"
    response = requests.get(url, headers=headers)
    return response


def get_address_brc20_history_by_ticker(address, ticker, type, start=0, limit=100):
    """
    type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
    """
    url = (
        URL
        + f"address/{address}/brc20/{ticker}/history"
        + f"?type={type}&start={start}&limit={limit}"
    )
    response = requests.get(url, headers=headers)
    return response


def get_transferable_inscriptions(address, ticker):
    url = URL + f"address/{address}/brc20/{ticker}/transferable-inscriptions"
    response = requests.get(url, headers=headers)
    return response


# Load environment variables from .env file
load_dotenv()


class UnisatAPI:
    def __init__(self):
        api_key = os.environ.get("UNISAT_API_KEY")
        if api_key is None:
            raise ValueError("UNISAT_API_KEY environment variable is not set")
        self.base_url = "https://open-api.unisat.io/v1/indexer/"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        self.api_key = api_key

    def _make_request(self, endpoint, params=None):
        url = self.base_url + endpoint
        response = requests.get(url, headers=self.headers, params=params)
        return response

    def get_best_block_height(self):
        return self._make_request("brc20/bestheight")

    def get_blockchain_info(self):
        return self._make_request("blockchain/info")

    def get_block_txs(self, height):
        return self._make_request(f"block/{height}/txs")

    def get_tx_info(self, txid):
        return self._make_request(f"tx/{txid}")

    def get_inscription_utxo(self, address):
        return self._make_request(f"address/{address}/inscription-utxo-data")

    def get_inscription_info(self, inscriptionId):
        return self._make_request(f"inscription/info/{inscriptionId}")

    def get_brc20_list(self, start=0, limit=100):
        return self._make_request(f"brc20/list", {"start": start, "limit": limit})

    def get_brc20_status(self, start=0, limit=10, sort="holders", complete="yes"):
        """
        sort by (holders/deploy/minted/transactions)
        filter by (yes/no)
        """
        return self._make_request(
            f"brc20/status",
            {"start": start, "limit": limit, "sort": sort, "complete": complete},
        )

    def get_brc20_ticker_info(self, ticker):
        return self._make_request(f"brc20/{ticker}/info")

    def get_brc20_holders(self, ticker):
        return self._make_request(f"brc20/{ticker}/holders")

    def get_brc20_ticker_history(self, ticker, height, type, start=0, limit=100):
        """
        type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
        """
        return self._make_request(
            f"brc20/{ticker}/history",
            {"type": type, "start": start, "height": height, "limit": limit},
        )

    def get_history_by_height(self, height, start=0, limit=100):
        return self._make_request(
            f"brc20/history-by-height/{height}", {"start": start, "limit": limit}
        )

    def get_brc20_tx_history(self, ticker, txid, type, start=0, limit=100):
        return self._make_request(
            f"brc20/{ticker}/tx/{txid}/history",
            {"type": type, "start": start, "limit": limit},
        )

    def get_address_brc20_summary(self, address, start=0, limit=100):
        return self._make_request(
            f"address/{address}/brc20/summary", {"start": start, "limit": limit}
        )

    def get_address_brc20_summary_by_height(self, address, height, start=0, limit=100):
        return self._make_request(
            f"address/{address}/brc20/summary-by-height/{height}",
            {"start": start, "limit": limit},
        )

    def get_address_brc20_ticker_info(self, address, ticker):
        return self._make_request(f"address/{address}/brc20/{ticker}/info")

    def get_address_brc20_history(self, address, start=0, limit=100):
        return self._make_request(
            f"address/{address}/brc20/history", {"start": start, "limit": limit}
        )

    def get_address_brc20_history_by_ticker(
        self, address, ticker, type, start=0, limit=100
    ):
        """
        type: inscribe-deploy, inscribe-mint, inscribe-transfer, transfer, send, receive
        """
        return self._make_request(
            f"address/{address}/brc20/{ticker}/history",
            {"type": type, "start": start, "limit": limit},
        )

    def get_transferable_inscriptions(self, address, ticker):
        return self._make_request(
            f"address/{address}/brc20/{ticker}/transferable-inscriptions"
        )


def main():
    unisat_api = UnisatAPI()
    # print(unisat_api.get_best_block_height().json())
    response = unisat_api.get_brc20_ticker_history("ordi", 826827, "inscribe-transfer")
    print(response.json()["data"])
    parent_directory = os.path.dirname(os.path.abspath(__file__))
    json_directory = os.path.join(parent_directory, "json")
    os.makedirs(json_directory, exist_ok=True)
    json_file_path = os.path.join(json_directory, "get_brc20_tx_history.json")
    with open(json_file_path, "w") as file:
        json.dump(response.json()["data"], file, indent=4)


if __name__ == "__main__":
    main()
