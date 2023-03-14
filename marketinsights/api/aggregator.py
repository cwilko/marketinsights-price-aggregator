
import marketinsights.connectors as connectors
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

    ################################################
    # getData
    ################################################
    #
    # - Data is inclusive of the start and end timestamps
    # - Aggregation can only occur if a sample_unit is provided. Otherwise results are a mixture
    # of different periods which are indistinguishable. For better results use Aggregate=False.
    # - Upsampling is not supported
    # - Merging of data is carried out in the order defined within the config to provide maximum flexibility.
    # - Sources are resampled ahead of any merge
    # - TODO Option 1: Enforce resampling order, e.g. D > H > 5min
    # - TODO Option 2: Combine merge with up (to common denominator) & down (to target) resample. To achieve a fine-grained merge.
    ################################################

    def getData(self, mkts=None, sample_unit=None, start="1979-01-01", end="2050-01-01", records=200, aggregate=True, debug=False):

        if aggregate and not sample_unit:
            raise Exception("Resample unit required for aggregation")

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

                        if sample_unit and sample_unit != source["sample_unit"]:  # Perform resampling

                            if pd.tseries.frequencies.to_offset(sample_unit) < pd.tseries.frequencies.to_offset(source["sample_unit"]):
                                raise Exception("Upsampling is not supported")

                            # Resample all to dataset sample unit (to introduce nans in all missing periods)
                            #tsData = ppl.resample(tsData, source["sample_unit"], debug)

                            # Resample to the requested unit
                            tsData = ppl.resample(tsData, sample_unit, debug)

                            # 06/06/18
                            # Remove NaNs to remove partial NaN entries
                            #tsData = ppl.removeNaNs(tsData)

                            # 01/03/23 - Remove nans from final output (and to avoid use of combine_first)
                            # Resample again to reintroduce nans in all time periods
                            # tsData = ppl.resample(tsData, sample_unit, debug)

                        tsData = ppl.removeNaNs(tsData)

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
