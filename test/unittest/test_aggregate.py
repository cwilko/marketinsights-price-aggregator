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

        marketData = aggregator.getData(["DOW"], "H", start, end, debug=False).round(1)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData, index=True).values).hexdigest()

        self.assertEqual(dataHash, "6698c9e3faf242af2130c45f2f4c178e")


class Aggregate(unittest.TestCase):

    def test_aggregate_yahoo(self):

        with open(dir + "/data/config2.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-01-05"

        marketData = aggregator.getData(["DOW", "IBM"], "D", start, end, debug=False).round(1)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData, index=True).values).hexdigest()

        self.assertEqual(dataHash, "5c61ad352db34a70e7db85b8dc1fbe4f")

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

        marketData = aggregator.getData(["DOW", "SPY"], "D", debug=False).round(1)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData, index=True).values).hexdigest()

        self.assertEqual(dataHash, "8bab1920a7ef97efeb7dfae254b26355")


if __name__ == '__main__':
    unittest.main()
