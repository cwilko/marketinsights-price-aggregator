
import MIPriceAggregator.connectors as connectors
import quantutils.dataset.pipeline as ppl
import pandas as pd


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
            self.datasources[datasource["ID"]] = MarketDataSource(connectorClass=datasource["class"], connName=datasource["ID"], options=datasource["opts"])

    def getData(self, mkts, sample_unit, start="1979-01-01", end="2050-01-01", records=200, debug=False):

        marketData = pd.DataFrame(pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID'])))

        for datasource in self.config:

            mds = self.datasources[datasource["ID"]]

            markets = [market for market in datasource["markets"] if market["ID"] in mkts]

            data = mds.getData(markets, start, end, records, debug)

            for market in markets:

                for source in market["sources"]:

                    tsData = data[data.index.get_level_values('ID') == source["ID"]]

                    if not tsData.empty:

                        tsData = tsData \
                            .reset_index() \
                            .set_index("Date_Time")[["Open", "High", "Low", "Close"]]

                        # 28/6/21 Move this to before data is saved for performance reasons
                        # Resample all to dataset sample unit (to introduce nans in all missing periods)
                        tsData = ppl.resample(tsData, source["sample_unit"], debug)

                        # Resample to the requested unit
                        tsData = ppl.resample(tsData, sample_unit, debug)

                        # 06/06/18
                        # Remove NaNs and resample again, to remove partial NaN entries before merging
                        tsData = ppl.removeNaNs(tsData)
                        tsData = ppl.resample(tsData, sample_unit, debug)

                        tsData = tsData \
                            .assign(ID=market["ID"]) \
                            .reset_index() \
                            .set_index(["Date_Time", "ID"])

                        marketData = ppl.merge(tsData, marketData)

        return marketData.sort_index()
