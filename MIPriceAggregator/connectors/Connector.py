import numpy as np


class Connector:

    def setState(self, state):
        pass

    def getData(self, markets, start, end, records, debug):
        pass

    def getOptions(self, chain, start, end, records, debug):
        pass

    def addUnderlyingValues(self, optionData, marketData, underlyingSymbol):

        if marketData is None:
            return np.nan

        underlying = marketData[marketData["ID"] == underlyingSymbol]
        return optionData.join(underlying, lsuffix='_caller', on="Date_Time")["Close"]
