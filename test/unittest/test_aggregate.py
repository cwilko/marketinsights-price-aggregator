import unittest
import os
import pandas as pd
import numpy as np
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
        dataHash = pd.util.hash_pandas_object(marketData, hash_key='0123456789123456').astype(float).sum()

        self.assertEqual(dataHash, 4.4964059088439654e+23)


class Aggregate(unittest.TestCase):

    def test_aggregate_yahoo(self):

        with open(dir + "/data/config2.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData(["DOW", "IBM"], "D", start, end, debug=False)
        dataHash = pd.util.hash_pandas_object(marketData, hash_key='0123456789123456').astype(float).sum()

        self.assertEqual(dataHash, 3.792534780145306e+22)

    def test_aggregate_barchart(self):

        with open(dir + "/data/config3.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData(["WTICrudeOil"], "D", records=50, debug=False)
        dataHash = pd.util.hash_pandas_object(marketData, hash_key='0123456789123456').astype(float).sum()

        self.assertEqual(marketData.dropna().shape, (50, 4))

    def test_aggregate_OHLC(self):

        with open(dir + "/data/config4.json") as json_file:
            data_config = json.load(json_file)

        for i in range(len(data_config)):
            data_config[i]["opts"]["root"] = dir + "/data"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData(["DOW", "SPY"], "D", debug=False)
        dataHash = pd.util.hash_pandas_object(marketData, hash_key='0123456789123456').astype(float).sum()

        self.assertEqual(dataHash, 1.158500396549419e+22)


if __name__ == '__main__':
    unittest.main()
