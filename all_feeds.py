from feeds.crypto_indices import mcap1000
from feeds import test_feed
from feeds.node_monkes import NodeMonkes

Test = test_feed.Test
MCAP1000 = mcap1000.MCAP1000

#NOTE: this is a dict of all feed classes that SIWA can run, keyed by feed name
#     this is used in endpoint.py to route requests to the correct feed
#
#TO ENABLE OR DISABLE A FEED, ADD OR REMOVE IT FROM THIS DICT
all_feeds = {
    Test.NAME: Test,
    MCAP1000.NAME: MCAP1000,
    NodeMonkes.NAME: NodeMonkes
    }
