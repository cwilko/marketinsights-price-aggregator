
import MIPriceAggregator.connectors as connectors
import quantutils.dataset.pipeline as ppl
import pandas


class MarketDataSource:

    def __init__(self, connectorClass, connName="-", tz="UTC", options={}):
        self.dataConnector = self.getConnector(connectorClass, connName, tz, options)

    def getConnector(self, connClass, connName, tz, options):
        connectorClass = getattr(connectors, connClass)
        connectorInstance = connectorClass(connName, tz, options)
        return connectorInstance

    def getData(self, market, source, start="1979-01-01", end="2050-01-01", records=200, debug=False):
        return self.dataConnector.getData(market, source, start, end, records, debug)

    def getOptionData(self, chain, start="1979-01-01", end="2050-01-01", records=200, debug=False):
        return self.dataConnector.getOptionData(chain, start, end, records, debug)


class MarketDataAggregator:

    def __init__(self, datasources):
        self.datasources = datasources

    def getData(self, market, sources, sample_unit, start="1979-01-01", end="2050-01-01", records=200, debug=False):

        marketData = None
        ID = ["Agg"]

        for datasource in self.datasources:

            for source in sources:

                data = datasource.getData(market, source, start, end, records, debug)
                ID.append(source["ID"])

                if not data.empty:

                    tsData = data[data.index.get_level_values('ID') == source["ID"]] \
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

                    if marketData is None:
                        marketData = pandas.DataFrame()

                    marketData = ppl.merge(tsData, marketData)

        if marketData is not None:
            marketData["ID"] = ':'.join(ID)
            marketData = marketData.reset_index().set_index(["Date_Time", "ID"])

        return marketData
