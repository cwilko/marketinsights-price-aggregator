import pandas as pd
import numpy as np
import os
import quantutils.dataset.pipeline as ppl
from marketinsights.connectors.Connector import Connector


class OHLCFileParser(Connector):

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

        DS_path = options["root"] + "/" + dsName + "/"
        self.SRC_path = DS_path + "raw/"

    def getData(self, markets, start, end, records, debug):
        marketData = pd.DataFrame(pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID'])))

        for market in markets:
            for source in market["sources"]:
                data = self.getSourceData(market, source, start, end, records, debug)
                if not data.empty:
                    marketData = ppl.merge(data, marketData)
        return marketData

    def getSourceData(self, market, source, start, end, records, debug):

        options = self.options

        existingData = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

        # Loop over any source files...
        for infile in os.listdir(self.SRC_path):

            if infile.lower().startswith(source["ID"].lower()):

                # Load RAW data (assume CSV)
                newData = pd.read_csv(self.SRC_path + infile,
                                      index_col=options["index_col"],
                                      parse_dates=options["parse_dates"],
                                      header=None,
                                      names=["Date", "Time", "Open", "High", "Low", "Close"],
                                      usecols=range(0, 6),
                                      skiprows=options["skiprows"],
                                      dayfirst=options["dayfirst"],
                                      float_precision="high"
                                      )

                if newData is not None:

                    if not newData.empty:

                        newData = newData \
                            .assign(ID=source["ID"]) \
                            .reset_index() \
                            .set_index(["Date_Time", "ID"]) \
                            .astype(dtype={"Open": np.float64, "High": np.float64, "Low": np.float64, "Close": np.float64}) \
                            [["Open", "High", "Low", "Close"]]

                        newData = ppl.localize(newData, self.tz, "UTC")
                        newData = newData.loc[pd.IndexSlice[start:end, :], :]

                        existingData = ppl.merge(newData, existingData)

        return existingData
