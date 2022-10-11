import json
import os
import quantutils.dataset.pipeline as ppl
from quantutils.api.datasource import MarketDataStore
import MIPriceAggregator.connectors as connectors
import pandas as pd
import numpy as np
import time
from datetime import datetime, date, timedelta


def getConnector(connClass, connName, tz, options):
    connectorClass = getattr(connectors, connClass)
    connectorInstance = connectorClass(connName, tz, options)
    return connectorInstance


def appendOptionChainPrices(mds, ds_file):

    datasources = json.load(open(ds_file))
    mdsKeys = mds.getKeys()

    for datasource in datasources:

        dataConnector = getConnector(datasource["class"], datasource["ID"], datasource["timezone"], datasource["opts"])

        for market in datasource["markets"]:

            options = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))
            if "optionChains" in market:

                for optionChain in market["optionChains"]:

                    # Get todays prices from optionChain
                    print("Requesting " + optionChain["ID"])
                    optionData = dataConnector.getOptions(optionChain, appendUnderlying=False, debug=False)

                    if optionData is not None:

                        optionData = optionData.sort_values(["expiry", "strike"])[["Open", "High", "Low", "Close", "Volume", "OpenInterest"]]
                        options = ppl.merge(options, optionData)

                if mds is not None and market["ID"] in mdsKeys:
                    print("Adding option data to " + market["ID"] + " table")
                    mds.append(market["ID"], options, update=True, debug=True)

    return options


def fetchHistoricalData(mds, ds_file, start="1979-01-01", end="2050-01-01", records=200, delta=False, newOnly=False, debug=False):

    datasources = json.load(open(ds_file))

    for datasource in datasources:

        dataConnector = getConnector(datasource["class"], datasource["ID"], datasource["timezone"], datasource["opts"])

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

                newData = dataConnector.getData(market, source, start, end, records)

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
    mds = MarketDataStore(remote=True, location="http://pricestore.192.168.1.203.nip.io")

    fetchHistoricalData(mds, ds_location, start=str(date.today()), end=str(date.today() + timedelta(days=1)), records=1, debug=True)
    appendOptionChainPrices(mds, ds_location)
    print("Updates complete at " + str(datetime.utcnow()))
