import numpy as np


class Connector:

    def setState(self, state):
        pass

    def getData(self, market, source, start, end, records):
        pass

    def getOptions(self, market, chain, appendUnderlying, start, end):
        pass

    def addUnderlyingValues(self, optionData, underlying):
        return optionData.join(underlying, lsuffix='_caller', on="Date_Time")["Close"]
