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
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData, index=True).values).hexdigest()

        self.assertEqual(dataHash, "39fa7ffa8e49c451a6759790d8bf86eb")


class Aggregate(unittest.TestCase):

    def test_aggregate_yahoo(self):

        with open(dir + "/data/config2.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData(["DOW", "IBM"], "D", start, end, debug=False).apply(np.floor)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData, index=True).values).hexdigest()

        self.assertEqual(dataHash, "39fda047652c4271aa174e6374246eca")

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

        marketData = aggregator.getData(["DOW", "SPY"], "D", debug=False).apply(np.floor)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData, index=True).values).hexdigest()

        self.assertEqual(dataHash, "a5fd8acfa2948eccb3ddd41c0cbde967")


if __name__ == '__main__':
    unittest.main()
