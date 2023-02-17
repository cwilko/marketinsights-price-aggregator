import unittest
import os
import pandas as pd
import numpy as np
import hashlib
import pytest
import json
from MIPriceAggregator.api.aggregator import MarketDataAggregator

dir = os.path.dirname(os.path.abspath(__file__))


class LocalAggregate(unittest.TestCase):

    def test_local_aggregate_MDS(self):

        with open(dir + "/data/config1.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData(["DOW"], "H", start, end, debug=False)
        dataHash = hashlib.md5(marketData.values.flatten().data).hexdigest()

        self.assertEqual(dataHash, "784b519199b6fb664efb666a3513b2e2")


class Aggregate(unittest.TestCase):

    def test_hash(self):
        hashstr = hashlib.md5("IAMACAT".encode('utf-8')).hexdigest()
        self.assertEqual(hashstr, "9f26f6f940ff221a89f8190447140258")

    def test_aggregate_yahoo(self):

        with open(dir + "/data/config2.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData(["DOW", "IBM"], "D", start, end, debug=False)
        dataHash = hashlib.md5(marketData.values.flatten().data).hexdigest()

        self.assertEqual(dataHash, "bac529e3220f4aa1bfd02169f5778903")

    def test_aggregate_barchart(self):

        with open(dir + "/data/config3.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData(["WTICrudeOil"], "D", records=50, debug=False)

        self.assertEqual(marketData.dropna().shape, (50, 4))

    def test_aggregate_OHLC(self):

        with open(dir + "/data/config4.json") as json_file:
            data_config = json.load(json_file)

        for i in range(len(data_config)):
            data_config[i]["opts"]["root"] = dir + "/data"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData(["DOW", "SPY"], "D", debug=False)
        dataHash = hashlib.md5(marketData.values.flatten().data).hexdigest()

        self.assertEqual(dataHash, "b27fda3b1b32bd031ff25ddb591f7bc7")


if __name__ == '__main__':
    unittest.main()
