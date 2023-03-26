import unittest
import os
import numpy as np
import pandas as pd
import hashlib
import pytest
import json
from marketinsights.api.aggregator import MarketDataAggregator
import marketinsights.utils.store as priceStore
from marketinsights.remote.datastore import MIDataStoreRemote

dir = os.path.dirname(os.path.abspath(__file__))
# 9b90de1971f03ce236c714153a9dd75e with precision="legacy"
sharedDigest = "a42b8d7f7feca51b5f86bdbb3c3ba0f6"
# 8525c634bf8e7f1d80b0213b319802df with precision="legacy"
sharedRawDigest = "7ce4bbd79446ab1a96cadc1b70e159a2"
start = "2016-04-15"
end = "2016-04-25"


class TestAggregate:

    @pytest.fixture(scope="class")
    def OHLCData(self):

        print("\nConstructing OHLC data...")
        with open(dir + "/data/config4.json") as json_file:
            data_config = json.load(json_file)

        for i in range(len(data_config)):
            data_config[i]["opts"]["root"] = dir + "/data"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        return aggregator.getData(aggregate=False)

    def test_local_aggregate_hourly_MDS(self):

        with open(dir + "/data/config1.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-02"

        marketData = aggregator.getData(["DOW"], "H", start, end, debug=False)
        print(marketData)
        dataHash = hashlib.md5(marketData.values.flatten()).hexdigest()
        assert marketData.shape == (20431, 4)
        assert dataHash == "19a8fb08d5d47d90b39fc6a5220379ca"

    def test_local_raw_MDS_and_save(self):

        with open(dir + "/data/config1.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-03-18"

        marketData = aggregator.getData(start=start, end=end, aggregate=False)
        assert marketData.shape == (116444, 4)
        assert hashlib.md5(marketData.values.flatten()).hexdigest() == "6c9b66ecec44601c4ddc349975cd74c9"

        # Now try to save the raw data
        mds = MIDataStoreRemote(location="http://pricestore.192.168.1.203.nip.io")
        savedData = priceStore.saveData(mds=mds, data=marketData, dry_run=True, delta=True)
        print(marketData)
        assert savedData.shape == (0, 0)

    def test_local_aggregate_MDS(self):

        with open(dir + "/data/config1.json") as json_file:
            data_config = json.load(json_file)

        # Get data from MDS
        MDS_aggregator = MarketDataAggregator(data_config)

        MDSData = MDS_aggregator.getData("DOW", "D", start, end, debug=False)

        dataHash = hashlib.md5(pd.util.hash_pandas_object(MDSData).values.flatten()).hexdigest()
        print(MDSData["Close"].values[0])
        assert MDSData.shape == (8, 4)
        # Note this must match the OHLC test hexdigest
        assert dataHash == sharedDigest

    def test_aggregate_yahoo(self):

        with open(dir + "/data/config2.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-02"
        end = "2018-08-03"

        marketData = aggregator.getData(sample_unit="D", start=start, end=end, debug=False).apply(np.floor)

        print(marketData)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()
        assert marketData.shape == (2814, 4)
        assert dataHash == "88541f70e5417c0330b5793db0b76f99"

    def test_aggregate_barchart(self):

        with open(dir + "/data/config3.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)
        marketData = aggregator.getData(["WTICrudeOil"], "D", records=50, debug=False)

        assert marketData.dropna().shape == (50, 4)

    def test_aggregate_OHLC(self):

        with open(dir + "/data/config4.json") as json_file:
            data_config = json.load(json_file)

        for i in range(len(data_config)):
            data_config[i]["opts"]["root"] = dir + "/data"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData("DOW", "D", start, end, debug=False)

        print(marketData)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()
        assert marketData.shape == (8, 4)
        assert dataHash == sharedDigest

    def test_raw_OHLC(self, OHLCData):

        marketData = OHLCData
        print(marketData)

        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()

        assert marketData.shape == (83303, 4)
        assert dataHash == sharedRawDigest

    def test_aggregate_PKL(self, OHLCData):

        OHLCData.droplevel("mID").rename_axis(index={'sID': 'ID'}).sort_index().to_pickle(dir + "/data/test2.pkl")

        with open(dir + "/data/config5.json") as json_file:
            data_config = json.load(json_file)
        data_config[0]["opts"]["path"] = dir + "/data/test2.pkl"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData("DOW", "D", start, end, debug=False)

        print(marketData)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()
        assert marketData.shape == (8, 4)
        assert dataHash == sharedDigest

    def test_raw_PKL(self, OHLCData):

        OHLCData.droplevel("mID").rename_axis(index={'sID': 'ID'}).sort_index().to_pickle(dir + "/data/test.pkl")

        with open(dir + "/data/config5.json") as json_file:
            data_config = json.load(json_file)
        data_config[0]["opts"]["path"] = dir + "/data/test.pkl"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData(aggregate=False)
        print(marketData)

        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()

        assert marketData.shape == (83303, 4)
        assert dataHash == sharedRawDigest

    def test_aggregate_MEM(self, OHLCData):

        with open(dir + "/data/config6.json") as json_file:
            data_config = json.load(json_file)

        data_config[0]["opts"]["data"] = OHLCData.droplevel("mID").rename_axis(index={'sID': 'ID'}).sort_index()

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData("DOW", "D", start, end, debug=False)

        print(marketData)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()
        assert marketData.shape == (8, 4)
        assert dataHash == sharedDigest

    def test_raw_MEM(self, OHLCData):

        with open(dir + "/data/config6.json") as json_file:
            data_config = json.load(json_file)

        data_config[0]["opts"]["data"] = OHLCData.droplevel("mID").rename_axis(index={'sID': 'ID'}).sort_index()

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData(aggregate=False)
        print(marketData)

        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()

        assert marketData.shape == (83303, 4)
        assert dataHash == sharedRawDigest


if __name__ == '__main__':
    unittest.main()
