import pandas
import os
import quantutils.dataset.pipeline as ppl
from MIPriceAggregator.connectors.Connector import Connector


class OHLCFileParser(Connector):

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

        DS_path = options["root"] + "/" + dsName + "/"
        self.SRC_path = DS_path + "raw/"

    def getData(self, market, source, start="1979-01-01", end="2050-01-01", records=0):

        options = self.options

        existingData = pandas.DataFrame(index=pandas.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

        # Loop over any source files...
        for infile in os.listdir(self.SRC_path):

            if infile.lower().startswith(source["ID"].lower()):

                # Load RAW data (assume CSV)
                newData = pandas.read_csv(self.SRC_path + infile,
                                          index_col=options["index_col"],
                                          parse_dates=options["parse_dates"],
                                          header=None,
                                          names=["Date", "Time", "Open", "High", "Low", "Close"],
                                          usecols=range(0, 6),
                                          skiprows=options["skiprows"],
                                          dayfirst=options["dayfirst"]
                                          )

                if newData is not None:
                    newData = ppl.cropDate(newData, start, end)

                    if not newData.empty:

                        newData = newData \
                            .assign(ID=source["ID"]) \
                            .reset_index() \
                            .set_index(["Date_Time", "ID"]) \
                            .astype(dtype={"Open": "Float64", "High": "Float64", "Low": "Float64", "Close": "Float64"}) \
                            [["Open", "High", "Low", "Close"]]

                        newData = ppl.localize(newData, self.tz, "UTC")

                        existingData = ppl.merge(newData, existingData)

        return existingData
