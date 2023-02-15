import unittest
import os
import pandas as pd
import numpy as np
import pytest
from MIPriceAggregator.api.aggregator import MarketDataAggregator

dir = os.path.dirname(os.path.abspath(__file__))


class Aggregate(unittest.TestCase):

    data_config = [
        {
            "ID": "MDS",
            "class": "MDSConnector",
            "opts": {
                "remote": True,
                "location": "http://pricestore.192.168.1.203.nip.io"
            },
            "timezone": "UTC",
            "markets": [
                {
                    "ID": "DOW",
                    "sources": [
                        {
                            "ID": "WallSt-hourly",
                            "sample_unit": "H"
                        },
                        {
                            "ID": "D&J-IND",
                            "sample_unit": "5min"
                        }
                    ]
                }
            ]

        }
    ]

    def test_aggregate_DOW(self):

        # Get Market Data
        aggregator = MarketDataAggregator(self.data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData("DOW", "H", start, end, debug=True)
        dataHash = pd.util.hash_pandas_object(marketData, hash_key='0123456789123456').astype(float).sum()

        self.assertEqual(dataHash, 4.4964059088439654e+23)

if __name__ == '__main__':
    unittest.main()
