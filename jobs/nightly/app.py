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


def fetchHistoricalData(mds, ds_file, start="1979-01-01", end="2050-01-01", records=200, delta=False):

    # Loop over datasources...
    # TODO: In chronological order

    datasources = json.load(open(ds_file))

    data = pd.DataFrame()

    for datasource in datasources:

        dataConnector = getConnector(datasource["class"], datasource["name"], datasource["timezone"], datasource["options"])

        for market in datasource["markets"]:

            for source in tqdm(market["sources"]):

                if delta:
                    try:
                        start = mds.aggregate([source["name"]], source["sample_unit"]).index[-1].strftime('%Y-%m-%d')
                    except:
                        print("Could not find " + source["name"])
                        start = "1979-01-01"

                    newData = dataConnector.getData(market, source, start)
                else:
                    newData = dataConnector.getData(market, source, start, end, records)

                if newData is not None:

                    print("Adding " + source["name"] + " to " + market["name"] + " table")

                    if mds is not None:
                        mds.append(source["name"], newData, source["sample_unit"], update=True)

                    data = ppl.merge(data, newData)
    return data


def fetchOptionData(datasources, start="1979-01-01", end="2050-01-01"):

    options = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'epic']))

    for datasource in datasources:

        dataConnector = getConnector(datasource["class"], datasource["name"], datasource["timezone"], datasource["opts"])

        for market in datasource["markets"]:

            for optionChain in tqdm(market["optionChains"]):

                optionData = dataConnector.getOptions(optionChain, appendUnderlying=False, start=start, end=end)

                if optionData is not None:
                    print("Adding " + optionChain["name"] + " to " + market["name"] + " table")

                    optionChain["options"] = json.loads(optionData.reset_index()[["epic", "instrumentName", "strike", "type"]].to_json(orient="records"))
                    options = ppl.merge(options, optionData)

        market["sources"].extend(json.loads(options.reset_index().assign(name=lambda x: x["epic"]).assign(sample_unit="D")[["name", "sample_unit"]].to_json(orient="records")))

    options = options.sort_values(ascending=[False, True, True], by=["Date_Time", "strike", "type"]).dropna()
    return options, datasources


def getOptionChains(datasource, root):

    dataConnector = getConnector(datasource["class"], datasource["name"], datasource["timezone"], datasource["options"])

    chains = dataConnector.getOptionChains(root)

    # Update expiry dates
    dates = []
    for _, chain in chains.iterrows():
        if np.isnan(chain["expiry"]):
            info = dataConnector.getOptionInfo(chain["name"])
            dates.append(info["contractExpirationDate"])
        else:
            dates.append(str(date.today() + timedelta(int(chain["expiry"]) + 1)))
    chains["expiry"] = dates

    return chains


def appendOptionChainPrices(mds, ds_file):

    datasources = json.load(open(ds_file))

    options = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'epic']))
    data = pd.DataFrame()

    for datasource in datasources:

        dataConnector = getConnector(datasource["class"], datasource["name"], datasource["timezone"], datasource["options"])

        for market in datasource["markets"]:

            for optionChain in tqdm(market["optionChains"]):

                # Get todays prices from optionChain
                optionData = dataConnector.getOption(optionChain, appendUnderlying=False)

                if optionData is not None:

                    print("Adding " + optionChain["name"] + " to " + market["name"] + " table")

                    df = optionData.replace(np.nan, 0).sort_values(["expiry", "strike"]).reset_index()
                    for i, option in df.iterrows():
                        newData = df.iloc[i:i + 1][["Date_Time", "Open", "High", "Low", "Close", "Volume", "OpenInterest"]].set_index("Date_Time")
                        if mds is not None:
                            mds.append(option["epic"], newData, "D", update=True)

                    options = ppl.merge(options, optionData)
    return options

# run
if __name__ == '__main__':
    # Remote (cluster)
    mds = MarketDataStore(remote=True, location="http://pricestore.192.168.1.203.nip.io")
    #fetchHistoricalData(mds, "../datasources", "datasources.json")
    print(mds.get("D&J-IND"))
