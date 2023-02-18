import unittest
import os
import pandas as pd
import numpy as np
import hashlib
import pytest
import json
from MIPriceAggregator.api.aggregator import MarketDataAggregator
import MIPriceAggregator.utils.store as priceStore
from quantutils.api.datasource import MIDataStoreRemote

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
        print(marketData)
        dataHash = hashlib.md5(marketData.values.flatten()).hexdigest()
        self.assertEqual(marketData.shape, (48956, 4))
        self.assertEqual(dataHash, "784b519199b6fb664efb666a3513b2e2")

    def test_local_raw_MDS_and_save(self):

        with open(dir + "/data/config1.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData(start=start, end=end, aggregate=False)
        self.assertEqual(marketData.shape, (118797, 4))
        self.assertEqual(hashlib.md5(marketData.values.flatten()).hexdigest(), "8809f5ad69f8c2c86727ca4c67e0d3fe")

        # Now try to save the raw data
        mds = MIDataStoreRemote(location="http://pricestore.192.168.1.203.nip.io")
        savedData = priceStore.saveData(mds=mds, data=marketData, dry_run=True, delta=True)
        # print(savedData)
        self.assertEqual(savedData.shape, (83, 4))
        self.assertEqual(hashlib.md5(savedData.values.flatten()).hexdigest(), "cc8f7809f3af98721ff31ce539c10e48")


class Aggregate(unittest.TestCase):

    def test_aggregate_yahoo(self):

        with open(dir + "/data/config2.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03"

        marketData = aggregator.getData(sample_unit="D", start=start, end=end, debug=False).apply(np.floor)

        print(marketData)
        dataHash = hashlib.md5(marketData.values.flatten()).hexdigest()
        self.assertEqual(marketData.shape, (4078, 4))
        self.assertEqual(dataHash, "e4182e7fea2a3ac69a4b1976e901d350")

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
        print(marketData)
        dataHash = hashlib.md5(marketData.values.flatten()).hexdigest()
        self.assertEqual(marketData.shape, (1418, 4))
        self.assertEqual(dataHash, "b541f46cc367c3187614bbc34c928f0c")

    def test_raw_OHLC(self):

        with open(dir + "/data/config4.json") as json_file:
            data_config = json.load(json_file)

        for i in range(len(data_config)):
            data_config[i]["opts"]["root"] = dir + "/data"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData(aggregate=False)
        print(marketData)

        dataHash = hashlib.md5(marketData.values.flatten()).hexdigest()

        self.assertEqual(marketData.shape, (64908, 4))
        self.assertEqual(dataHash, "e5ee5ece2192a95581df2cef5aae129c")

if __name__ == '__main__':
    unittest.main()
