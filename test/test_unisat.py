import sys
sys.path.insert(0, 'E:\arman\siwa-lite\apis')
import os
from apis import unisat 
import constants as c
import unittest

class TestUnisat(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # if os.path.exists(Test.get_data_dir()):
        ...
        #     os.remove(Test.get_data_dir()) 

    @classmethod
    def tearDownClass(cls):
        ...

    def test_get_blockchain_info(self):
        self.assertListEqual(list(unisat.get_blockchain_info().json().keys()),
                ['code', 'msg', 'data']      
                )


if __name__ == '__main__':
    unittest.main()