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
        self.assertEqual(marketData.shape, (20424, 4))
        self.assertEqual(dataHash, "c1759b018498a26fd30ae67d727f668b")

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
        self.assertEqual(marketData.shape, (2814, 4))
        self.assertEqual(dataHash, "9375ba21062baa5507c82f30b69a1cc3")

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
        self.assertEqual(marketData.shape, (905, 4))
        self.assertEqual(dataHash, "a180491aea7707ea7dbf3ebc2d61d13e")

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
