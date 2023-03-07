from marketinsights.connectors.Connector import Connector
from datetime import datetime, date
from quantutils.api.datasource import MIDataStoreRemote
import quantutils.dataset.pipeline as ppl
import pandas as pd
import numpy as np
from dateutil import parser


class MDSConnector(Connector):

    def __init__(self, dsName, tz, opts):
        self.dsName = dsName
        self.opts = opts
        self.tz = tz
        self.marketData = None

        self.mds = MIDataStoreRemote(location=opts["location"])

    def setState(self, state):
        if "marketData" in state:
            self.marketData = state["marketData"]

    def getData(self, markets, start, end, records, debug):

        marketData = pd.DataFrame(pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID'])))

        for market in markets:
            for source in market["sources"]:
                data = self.getSourceData(market, source, start, end, records, debug)
                if not data.empty:
                    marketData = ppl.merge(data, marketData)

        return marketData

    def getSourceData(self, market, source, start, end, records, debug):
        return self.mds.aggregate(market["ID"], [source["ID"]], start, end)

    def getOptionData(self, chain, start, end, records, debug):

        underlying = self.marketData.loc[pd.IndexSlice[:, [chain["underlying"]]], :]

        data = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

        if "options" in chain:
            options = chain["options"]
            for option in options:
                #print("Adding {} to Option {}".format(option["ID"], chain["ID"]))
                optionData = self.marketData.loc[pd.IndexSlice[:, [option["ID"]]], :]
                optionData["instrumentName"] = option["instrumentName"]
                optionData["strike"] = float(option["strike"])
                optionData["type"] = option["type"]
                underlyingVals = underlying.loc[optionData.index.get_level_values(0), "Close"]
                optionData["underlying"] = np.nan
                optionData.loc[underlyingVals.index.get_level_values(0), "underlying"] = underlyingVals.values
                data = pd.concat([data, optionData])

            data["underlyingSymbol"] = chain["underlying"]
            data["ask"] = data["Close"]
            data["bid"] = data["Close"]
            data["expiry"] = datetime.strptime(chain["expiry"], '%Y-%m-%d')
            data["expiry"] = pd.to_datetime(data["expiry"], utc=True)
            data["timeToExpiry"] = [((exp.days * 24. * 3600. + exp.seconds + 3600) / (24. * 3600.)) for exp in data['expiry'].sub(data.index.get_level_values(0))]

        return data
