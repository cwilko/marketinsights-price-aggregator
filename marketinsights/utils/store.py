import quantutils.dataset.pipeline as ppl
from marketinsights.api.aggregator import MarketDataSource, MarketDataAggregator
import pandas as pd
import numpy as np
from datetime import datetime


def saveHistoricalOptionData(mds, datasources, start="1979-01-01", end="2050-01-01", records=200, refreshUnderyling=False, dry_run=False, debug=False):

    data = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

    # First ensure that all underlying date is updated
    if refreshUnderyling:
        aggregator = MarketDataAggregator(datasources)
        underlyingData = aggregator.getData(start=start, end=end, records=records, aggregate=False, debug=debug)
        saveData(mds, underlyingData)

    for datasource in datasources:

        ds = MarketDataSource(datasource["class"], datasource["ID"], datasource["timezone"], datasource["opts"])

        for market in datasource["markets"]:

            ds.setState({"marketData": mds.get(market["ID"])})

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


from datetime import datetime


def saveData(mds, data, delta=False, dry_run=False, debug=False):

    resultData = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

    for mID in data.index.get_level_values("mID").unique().values:

        for sID in data.xs(mID).index.get_level_values("sID").unique().values:

            # TODO Implement newOnly
            start = 0
            if delta:
                try:
                    oldData = mds.aggregate(mID, [sID])
                    start = oldData.index.get_level_values("Date_Time")[-1].strftime('%Y-%m-%d')
                    # print(start)
                except Exception as e:
                    print(e)
                    print("Could not find " + sID + " within table " + mID)

            newData = data.xs((mID, sID)).assign(ID=sID)[start:].set_index(["ID"], append=True)

            if newData is not None:

                print("Adding " + sID + " to " + mID + " table")

                if debug:
                    print(newData)

                if not dry_run:
                    mds.append(mID, newData, update=True)

                resultData = ppl.merge(newData, resultData)

    return resultData
