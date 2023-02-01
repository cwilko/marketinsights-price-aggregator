import json
import os
import quantutils.dataset.pipeline as ppl
from MIPriceAggregator.api.aggregator import MarketDataSource
from quantutils.api.datasource import MIDataStoreRemote
import pandas as pd
import numpy as np
import time
from datetime import datetime, date, timedelta


def fetchHistoricalOptionData(mds, ds_file, start="1979-01-01", end="2050-01-01", records=200, refreshUnderyling=False, dry_run=False, debug=False):

    datasources = json.load(open(ds_file))

    data = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

    # First ensure that all underlying date is updated
    if refreshUnderyling:
        fetchHistoricalData(mds, ds_file, start=start, end=end, records=records, debug=debug)

    for datasource in datasources:

        ds = MarketDataSource(datasource["class"], datasource["ID"], datasource["timezone"], datasource["opts"])

        for market in datasource["markets"]:

            dataConnector.setState({"marketData": mds.get(market["ID"])})

            if "optionChains" in market:

                for optionChain in market["optionChains"]:

                    # TODO: Implement newOnly

                    newData = ds.getOptionData(optionChain, start, end, records, debug=False)

                    if newData is not None:

                        print("Adding " + optionChain["ID"] + " to " + market["ID"] + " table")

                        if debug:
                            print(newData)

                        if not dry_run:
                            mds.append(market["ID"] + "_Options", newData, update=True)

                        data = ppl.merge(data, newData)
    return data


def fetchHistoricalData(mds, ds_file, start="1979-01-01", end="2050-01-01", records=200, delta=False, newOnly=False, debug=False):

    datasources = json.load(open(ds_file))

    for datasource in datasources:

        ds = MarketDataSource(datasource["class"], datasource["ID"], datasource["timezone"], datasource["opts"])

        for market in datasource["markets"]:

            data = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

            for source in market["sources"]:

                # TODO Implement newOnly

                if delta:
                    try:
                        start = mds.aggregate(market["ID"], [source["ID"]]).index.get_level_values("Date_Time")[-1]
                        print(start)
                        records = (datetime.utcnow() - start.to_pydatetime().replace(tzinfo=None)).days + 1
                        print(records)
                        start = start.strftime('%Y-%m-%d')
                    except Exception as e:
                        print(e)
                        print("Could not find " + market["ID"])
                        start = "1979-01-01"

                newData = ds.getData(market, source, start, end, records)

                if newData is not None:

                    print("Adding " + source["ID"] + " to " + market["ID"] + " table")

                    if debug:
                        print(newData)

                    data = ppl.merge(data, newData)

            if mds is not None:
                mds.append(market["ID"], data, update=True)

    return data
# run
if __name__ == '__main__':

    # Get environment variables
    MI_DATASTORE_LOCATION = os.getenv('MI_DATASTORE_LOCATION')
    MI_DATASOURCE_LOCATION = os.environ.get('MI_DATASOURCE_LOCATION')
    MI_PRICE_STORE_URL = os.environ.get('MI_PRICE_STORE_URL')

    ds_location = "../datasources/datasources.json"

    # Local Options
    mds = MIDataStoreRemote(location="http://pricestore.192.168.1.203.nip.io")
    fetchHistoricalOptionData(mds, ds_location, start=str(date.today()), end=str(date.today() + timedelta(days=1)), records=1, refreshUnderyling=True, debug=True)
    print("Updates complete at " + str(datetime.utcnow()))
