import unittest
import os
import numpy as np
import pandas as pd
import hashlib
import pytest
import json
from marketinsights.api.aggregator import MarketDataAggregator
import marketinsights.utils.store as priceStore
from quantutils.api.datasource import MIDataStoreRemote

dir = os.path.dirname(os.path.abspath(__file__))
sharedDigest = "024ac9ff95bb1dc73854ea3f257cbc1c"


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
        assert marketData.shape == (20423, 4)
        assert dataHash == "451bd3a567e847f0cccb4bed2f9c7032"

    def test_local_raw_MDS_and_save(self):

        with open(dir + "/data/config1.json") as json_file:
            data_config = json.load(json_file)

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        start = "2013-01-01"
        end = "2018-08-03 00:00"

        marketData = aggregator.getData(start=start, end=end, aggregate=False)
        assert marketData.shape == (118797, 4)
        assert hashlib.md5(marketData.values.flatten()).hexdigest() == "8809f5ad69f8c2c86727ca4c67e0d3fe"

        # Now try to save the raw data
        mds = MIDataStoreRemote(location="http://localhost:8080")
        savedData = priceStore.saveData(mds=mds, data=marketData, dry_run=True, delta=True)
        print(savedData)
        assert savedData.shape == (83, 4)
        assert hashlib.md5(savedData.values.flatten()).hexdigest() == "cc8f7809f3af98721ff31ce539c10e48"

    def test_local_aggregate_MDS(self):

        with open(dir + "/data/config1.json") as json_file:
            data_config = json.load(json_file)

        start = "2016-10-05"
        end = "2016-10-25"

        # Get data from MDS
        MDS_aggregator = MarketDataAggregator(data_config)

        MDSData = MDS_aggregator.getData("DOW", "D", start, end, debug=False)

        dataHash = hashlib.md5(pd.util.hash_pandas_object(MDSData).values.flatten()).hexdigest()
        print(MDSData)
        assert MDSData.shape == (18, 4)
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

        start = "2016-10-05"
        end = "2016-10-25"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData("DOW", "D", start, end, debug=False)

        print(marketData)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()
        assert marketData.shape == (18, 4)
        assert dataHash == sharedDigest

    def test_raw_OHLC(self, OHLCData):

        marketData = OHLCData
        print(marketData)

        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()

        assert marketData.shape == (64908, 4)
        assert dataHash == "4ba7f99dc62027e26edfc59a7972ce59"

    def test_aggregate_PKL(self, OHLCData):

        OHLCData.droplevel("mID").rename_axis(index={'sID': 'ID'}).sort_index().to_pickle(dir + "/data/test2.pkl")

        with open(dir + "/data/config5.json") as json_file:
            data_config = json.load(json_file)
        data_config[0]["opts"]["path"] = dir + "/data/test2.pkl"

        start = "2016-10-05"
        end = "2016-10-25"

        # Get Market Data
        aggregator = MarketDataAggregator(data_config)

        marketData = aggregator.getData("DOW", "D", start, end, debug=False)

        print(marketData)
        dataHash = hashlib.md5(pd.util.hash_pandas_object(marketData).values.flatten()).hexdigest()
        assert marketData.shape == (18, 4)
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

        assert marketData.shape == (64908, 4)
        assert dataHash == "4ba7f99dc62027e26edfc59a7972ce59"


if __name__ == '__main__':
    unittest.main()
