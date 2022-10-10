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

        dataConnector = getConnector(datasource["class"], datasource["name"], datasource["timezone"], datasource["opts"])

        for market in datasource["markets"]:

            options = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

            for optionChain in market["optionChains"]:

                # Get todays prices from optionChain
                print("Requesting " + optionChain["name"])
                optionData = dataConnector.getOptions(optionChain, appendUnderlying=False, debug=False)

                if optionData is not None:

                    optionData = optionData.replace(np.nan, 0).sort_values(["expiry", "strike"])[["Open", "High", "Low", "Close", "Volume", "OpenInterest"]]

                    options = ppl.merge(options, optionData)

            if mds is not None and market["name"] in mdsKeys:
                print("Adding " + optionChain["name"] + " to " + market["name"] + " table")
                mds.append(market["name"], options, "D", update=True, debug=True)

    return options

# run
if __name__ == '__main__':

    # Get environment variables
    MI_DATASTORE_LOCATION = os.getenv('MI_DATASTORE_LOCATION')
    MI_DATASOURCE_LOCATION = os.environ.get('MI_DATASOURCE_LOCATION')
    MI_PRICE_STORE_URL = os.environ.get('MI_PRICE_STORE_URL')

    ds_location = "../datasources/datasources_BarChartOption.json"

    # Local Options
    mds = MarketDataStore(remote=True, location="http://pricestore.192.168.1.203.nip.io")
    options = appendOptionChainPrices(mds, ds_location)
