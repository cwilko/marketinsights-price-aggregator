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

        marketData = aggregator.getData(["DOW"], "H", start, end, debug=False).apply(np.floor)
        dataHash = hashlib.md5(marketData.values.flatten()).hexdigest()

        self.assertEqual(dataHash, "39fa7ffa8e49c451a6759790d8bf86eb")


class Aggregate(unittest.TestCase):

    def _test_aggregate_yahoo(self):

        with open(dir + "/data/config2.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData(["DOW", "IBM"], "D", start, end, debug=False).apply(np.floor)
        dataHash = hashlib.md5(marketData.values.flatten()).hexdigest()
        self.assertEqual(dataHash, "0c88e2885ebfefe9eec7935f9f3d560d")

        # marketData.to_pickle(dir + "/data/yahoo.pkl")
        # compare = pd.read_pickle(dir + "/data/yahoo.pkl")

        # print(marketData["2016-10-31":])

        # self.assertTrue(False)

    def _test_aggregate_barchart(self):

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

        marketData = aggregator.getData(["DOW", "SPY"], "D", start="2016-10-31", end="2016-11-06", debug=False)
        # marketData.to_pickle(dir + "/data/ohlc.pkl")
        compare = pd.read_pickle(dir + "/data/ohlc.pkl")

        print(pd.concat([marketData, compare]).drop_duplicates(keep=False))
        print(compare["2016-10-31":].dtypes)
        print(marketData["2016-10-31":].dtypes)

        self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
