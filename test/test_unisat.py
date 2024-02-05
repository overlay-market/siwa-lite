import sys
import os

# Add the parent directory of siwa-lite to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(parent_dir)

from apis.unisat import UnisatAPI
import constants as c
import unittest


class TestUnisat(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.UnisatAPI = UnisatAPI()

    @classmethod
    def tearDown(self):
        # Perform any cleanup or teardown actions if needed
        pass

    def test_get_blockchain_info(self):
        self.assertListEqual(
            list(self.UnisatAPI.get_blockchain_info().json()["data"].keys()),
            ['chain', 'blocks', 'headers', 'bestBlockHash', 'prevBlockHash', 'difficulty', 'medianTime', 'chainwork'],
        )

    def test_get_block_txs(self):
        height = 824631
        self.assertListEqual(
            list(self.UnisatAPI.get_block_txs(height).json()["data"][0].keys()),
            ['txid', 'nIn', 'nOut', 'size', 'witOffset', 'locktime', 'inSatoshi', 'outSatoshi', 'nNewInscription', 'nInInscription', 'nOutInscription', 'nLostInscription', 'timestamp', 'height', 'blkid', 'idx', 'confirmations'],
        )

    def test_get_block_txs(self):
        txid = '45a76470f80982d769b1974181cd4f7261084ac8db3dcb1cd4547f9fe91590cf'
        self.assertListEqual(
            list(self.UnisatAPI.get_tx_info(txid).json()["data"].keys()),
            ['txid', 'nIn', 'nOut', 'size', 'witOffset', 'locktime', 'inSatoshi', 'outSatoshi', 'nNewInscription', 'nInInscription', 'nOutInscription', 'nLostInscription', 'timestamp', 'height', 'blkid', 'idx', 'confirmations'],
        )

    def test_get_inscription_utxo(self):
        address = '1K6KoYC69NnafWJ7YgtrpwJxBLiijWqwa6'
        self.assertListEqual(
            list(self.UnisatAPI.get_inscription_utxo(address).json()["data"].keys()),
            ['cursor', 'total', 'totalConfirmed', 'totalUnconfirmed', 'totalUnconfirmedSpend', 'utxo'],
        )

    def test_get_inscription_info(self):
        inscriptionid = '75017937ad1de1f50709910aa5889be9c7d8f019a1c02922d291f9bfa9a8b0fei0'
        self.assertListEqual(
            list(self.UnisatAPI.get_inscription_utxo(inscriptionid).json()["data"].keys()),
            ['utxo', 'address', 'offset', 'inscriptionIndex', 'inscriptionNumber', 'inscriptionId', 'contentType', 'contentLength', 'contentBody', 'height', 'timestamp', 'inSatoshi', 'outSatoshi', 'brc20', 'detail'],
        )

    def test_get_brc20_list(self):
        start = 0
        limit = 100
        self.assertListEqual(
            list(self.UnisatAPI.get_brc20_list(start, limit).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_brc20_status(self):
        start = 0
        limit = 100
        sort = "holders"
        complete = "yes"
        self.assertListEqual(
            list(self.UnisatAPI.get_brc20_status(start, limit, sort, complete).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_brc20_ticker_info(self):
        ticker = "EFIL"
        self.assertListEqual(
            list(self.UnisatAPI.get_brc20_ticker_info(ticker).json()["data"].keys()),
            ['ticker', 'holdersCount', 'historyCount', 'inscriptionNumber', 'inscriptionId', 'max', 'limit', 'minted', 'totalMinted', 'confirmedMinted', 'confirmedMinted1h', 'confirmedMinted24h', 'mintTimes', 'decimal', 'creator', 'txid', 'deployHeight', 'deployBlocktime', 'completeHeight', 'completeBlocktime', 'inscriptionNumberStart', 'inscriptionNumberEnd'],
        )

    def test_get_brc20_holders(self):
        ticker = "EFIL"
        self.assertListEqual(
            list(self.UnisatAPI.get_brc20_holders(ticker).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    # Need to try with correct params
    # def test_get_brc20_ticker_history(self):
    #     ticker = "EFIL"
    #     txid = '45a76470f80982d769b1974181cd4f7261084ac8db3dcb1cd4547f9fe91590cf'
    #     type = "inscribe-deploy"
    #     start = 0
    #     limit = 100
    #     self.assertListEqual(
    #         list(self.UnisatAPI.get_brc20_ticker_history(ticker, txid, type, start, limit).json()["data"].keys()),
    #         ['height', 'total', 'start', 'detail'],
    #     )

    def test_get_history_by_height(self):
        height = 824631
        start = 0
        limit = 100
        self.assertListEqual(
            list(self.UnisatAPI.get_history_by_height(height, start, limit).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_brc20_tx_history(self):
        ticker = "EFIL"
        txid = '45a76470f80982d769b1974181cd4f7261084ac8db3dcb1cd4547f9fe91590cf'
        start = 0
        limit = 100
        self.assertListEqual(
            list(self.UnisatAPI.get_brc20_tx_history(ticker, txid, start, limit).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_address_brc20_summary(self):
        address = '1K6KoYC69NnafWJ7YgtrpwJxBLiijWqwa6'
        start = 0
        limit = 100
        self.assertListEqual(
            list(self.UnisatAPI.get_address_brc20_summary(address, start, limit).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_address_brc20_summary_by_height(self):
        address = '1K6KoYC69NnafWJ7YgtrpwJxBLiijWqwa6'
        height = 824631
        start = 0
        limit = 100
        self.assertListEqual(
            list(self.UnisatAPI.get_address_brc20_summary_by_height(address, height, start, limit).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_address_brc20_ticker_info(self):
        address = '1K6KoYC69NnafWJ7YgtrpwJxBLiijWqwa6'
        ticker = "EFIL"
        self.assertListEqual(
            list(self.UnisatAPI.get_address_brc20_ticker_info(address, ticker).json()["data"].keys()),
            ['ticker', 'overallBalance', 'transferableBalance', 'availableBalance', 'availableBalanceSafe', 'availableBalanceUnSafe', 'transferableCount', 'transferableInscriptions', 'historyCount', 'historyInscriptions'],
        )

    def test_get_address_brc20_history(self):
        address = '1K6KoYC69NnafWJ7YgtrpwJxBLiijWqwa6'
        start = 0
        limit = 100
        self.assertListEqual(
            list(self.UnisatAPI.get_address_brc20_history(address, start, limit).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_address_brc20_history_by_ticker(self):
        address = '1K6KoYC69NnafWJ7YgtrpwJxBLiijWqwa6'
        ticker = "EFIL"
        type = "inscribe-deploy"
        start = 0
        limit = 100
        self.assertListEqual(
            list(self.UnisatAPI.get_address_brc20_history_by_ticker(address, ticker, type, start, limit).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )
        
    def test_get_transferable_inscriptions(self):
        address = '1K6KoYC69NnafWJ7YgtrpwJxBLiijWqwa6'
        ticker = "EFIL"
        self.assertListEqual(
            list(self.UnisatAPI.get_transferable_inscriptions(address, ticker).json()["data"].keys()),
            ['height', 'total', 'start', 'detail'],
        )

    def test_get_collection_stats(self):
        collectionId = 'nodemonkes'
        self.assertIn(
            "floorPrice",
            list(self.UnisatAPI.get_collection_stats(collectionId).json()["data"].keys()),
        )
        collectionId = 'bitcoin-frogs'
        self.assertIn(
            "floorPrice",
            list(self.UnisatAPI.get_collection_stats(collectionId).json()["data"].keys()),
        )

if __name__ == "__main__":
    unittest.main()
