import pandas
import os
import quantutils.dataset.pipeline as ppl


class OHLCFileParser:

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

        DS_path = options["root"] + "/" + dsName + "/"
        self.SRC_path = DS_path + "raw/"

    def getData(self, source):

        options = self.options

        existingData = pandas.DataFrame()

        # Loop over any source files...
        for infile in os.listdir(self.SRC_path):

            if infile.lower().startswith(source["name"].lower()):

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

                    newData = ppl.localize(newData, self.tz, "UTC")

                    existingData = ppl.merge(newData, existingData)

        return existingData
