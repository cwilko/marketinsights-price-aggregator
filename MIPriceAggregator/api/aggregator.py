
import MIPriceAggregator.connectors as connectors
import quantutils.dataset.pipeline as ppl
import pandas as pd
from tqdm import tqdm


class MarketDataSource:

    def __init__(self, connectorClass, connName="-", tz="UTC", options={}):
        self.dataConnector = self.getConnector(connectorClass, connName, tz, options)

    def getConnector(self, connClass, connName, tz, options):
        connectorClass = getattr(connectors, connClass)
        connectorInstance = connectorClass(connName, tz, options)
        return connectorInstance

    def getData(self, markets, start="1979-01-01", end="2050-01-01", records=200, debug=False):
        return self.dataConnector.getData(markets, start, end, records, debug)

    def getSourceData(self, market, source, start="1979-01-01", end="2050-01-01", records=200, debug=False):
        return self.dataConnector.getSourceData(market, source, start, end, records, debug)

    def getOptionData(self, chain, start="1979-01-01", end="2050-01-01", records=200, debug=False):
        return self.dataConnector.getOptionData(chain, start, end, records, debug)

    def setState(self, state):
        return self.dataConnector.setState(state)


class MarketDataAggregator:

    datasources = {}

    def __init__(self, config):
        self.config = config
        for datasource in config:
            self.datasources[datasource["ID"]] = MarketDataSource(connectorClass=datasource["class"], connName=datasource["ID"], tz=datasource["timezone"], options=datasource["opts"])

    def getTestData(self, mkts=None, sample_unit=None, start="1979-01-01", end="2050-01-01", records=200, aggregate=True, debug=False):

        if aggregate:
            resultData = pd.DataFrame(pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'mID', u'Date_Time'])))
        else:
            resultData = pd.DataFrame(pd.DataFrame(index=pd.MultiIndex(levels=[[], [], []], codes=[[], [], []], names=[u'mID', u'sID', u'Date_Time'])))

        for datasource in self.config:

            mds = self.datasources[datasource["ID"]]

            markets = [market for market in datasource["markets"] if ((not mkts) or (market["ID"] in mkts))]

            data = mds.getData(markets, start, end, records, debug)

            for market in tqdm(markets):

                marketData = pd.DataFrame(pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'mID'])))

                for source in market["sources"]:

                    tsData = data[data.index.get_level_values('ID') == source["ID"]]

                    if not tsData.empty:

                        tsData = tsData \
                            .reset_index() \
                            .set_index("Date_Time")[["Open", "High", "Low", "Close"]]

                        if sample_unit:  # Perform resampling

                            # 28/6/21 Move this to before data is saved for performance reasons
                            # Resample all to dataset sample unit (to introduce nans in all missing periods)
                            tsData = ppl.resample(tsData, source["sample_unit"], debug)

                            # Resample to the requested unit
                            tsData = ppl.resample(tsData, sample_unit, debug)

                            # 06/06/18
                            # Remove NaNs and resample again, to remove partial NaN entries before merging
                            tsData = ppl.removeNaNs(tsData)
                            #tsData = ppl.resample(tsData, sample_unit, debug)

                        # Reset IDs
                        tsData["mID"] = market["ID"]

                        if aggregate:
                            tsData.set_index('mID', append=True, inplace=True)
                            marketData = ppl.merge(tsData, marketData)
                        else:
                            tsData["sID"] = source["ID"]
                            tsData.set_index(['mID', 'sID'], append=True, inplace=True)
                            resultData = ppl.merge(tsData, resultData)

                if aggregate and not marketData.empty:
                    resultData = ppl.merge(marketData, resultData)

        # if aggregate:
        #    resultData.set_index('sID', append=True, inplace=True)
        #    resultData = resultData.droplevel("sID")

        return resultData.sort_index()

    def getData(self, mkts=None, sample_unit=None, start="1979-01-01", end="2050-01-01", records=200, aggregate=True, debug=False):

        tsDatalist = []

        for datasource in self.config:

            mds = self.datasources[datasource["ID"]]

            markets = [market for market in datasource["markets"] if ((not mkts) or (market["ID"] in mkts))]

            data = mds.getData(markets, start, end, records, debug)

            for market in tqdm(markets):

                for source in market["sources"]:

                    tsData = data[data.index.get_level_values('ID') == source["ID"]]

                    if not tsData.empty:

                        tsData = tsData \
                            .reset_index() \
                            .set_index("Date_Time")[["Open", "High", "Low", "Close"]]

                        if sample_unit:  # Perform resampling

                            # Resample all to dataset sample unit (to introduce nans in all missing periods)
                            tsData = ppl.resample(tsData, source["sample_unit"], debug)

                            # Resample to the requested unit
                            tsData = ppl.resample(tsData, sample_unit, debug)

                            # 06/06/18
                            # Remove NaNs to remove partial NaN entries
                            tsData = ppl.removeNaNs(tsData)

                            # 01/03/23 - Remove nans from final output (and to avoid use of combine_first)
                            # Resample again to reintroduce nans in all time periods
                            # tsData = ppl.resample(tsData, sample_unit, debug)

                        # Reset IDs
                        tsData["mID"] = market["ID"]

                        if aggregate:
                            tsData.set_index('mID', append=True, inplace=True)
                        else:
                            tsData["sID"] = source["ID"]
                            tsData.set_index(['mID', 'sID'], append=True, inplace=True)

                        tsDatalist.append(tsData)

        if tsDatalist:
            if aggregate:
                resultData = pd.concat(tsDatalist)
                resultData = resultData[~resultData.index.duplicated(keep='first')]
                resultData = resultData.reorder_levels(["mID", "Date_Time"])
            else:
                resultData = pd.concat(tsDatalist)
                resultData = resultData.reorder_levels(["mID", "sID", "Date_Time"])

        return resultData.sort_index()
