from datetime import datetime, date
import quantutils.dataset.pipeline as ppl
from quantutils.api.datasource import MarketDataStore
import pandas as pd
import numpy as np
from dateutil import parser


class MDSConnector:

    def __init__(self, dsName, tz, opts):
        self.dsName = dsName
        self.opts = opts
        self.tz = tz

        self.mds = MarketDataStore(opts["path"])

    def getData(self, market, source, start="1979-01-01", end="2050-01-01", records=0):

        return self.mds.aggregate([source["name"]], source["sample_unit"], start, end)

    def getOptions(self, chain, appendUnderlying=True, start="1979-01-01", end="2050-01-01"):

        underlying = self.mds.aggregate([chain["underlying"]], self.opts["interval"], start, end)

        data = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'epic']))

        options = chain["options"]
        for option in options:
            print("Adding {} to Option {}".format(option["epic"], chain["name"]))
            optionData = self.mds.get(option["epic"])
            optionData["epic"] = option["epic"]
            optionData["instrumentName"] = option["instrumentName"]
            optionData["strike"] = option["strike"]
            optionData["type"] = option["type"]
            optionData["underlying"] = underlying.loc[optionData.index]["Close"]
            optionData = optionData.reset_index().set_index(["Date_Time", "epic"])
            data = ppl.merge(data, optionData)

        data["underlyingSymbol"] = chain["underlying"]
        data["ask"] = data["Close"]
        data["bid"] = data["Close"]
        data["expiry"] = datetime.strptime(chain["expiry"], '%Y-%m-%d')

        return data
