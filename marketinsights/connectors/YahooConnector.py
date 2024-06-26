import pandas as pd
import numpy as np
import yfinance as yf
import quantutils.dataset.pipeline as ppl
from marketinsights.connectors.Connector import Connector


class YahooConnector(Connector):

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

    def getSourceData(self, market, source, start, end, records, debug):
        # Extract ticker info
        tickers = [source["ID"]]
        return self.getYahooData(tickers, start, end, debug)

    def getData(self, markets, start, end, records, debug):
        # Extract ticker info
        tickers = []
        [tickers.extend([source["ID"] for source in market["sources"]]) for market in markets]
        return self.getYahooData(tickers, start, end, debug)

    def getYahooData(self, tickers, start, end, debug):
        data = yf.download(tickers=tickers, start=start, end=end, interval=self.options["interval"], prepost=False)

        # Workaround for yfinance not including ticker name when a single ticker
        if len(tickers) == 1:
            data.columns = pd.MultiIndex.from_tuples(list(zip(data.columns, tickers * len(data.columns))))

        # Adjust end date to be "inclusive" - to match pandas indexing
        if end.strip().find(" ") < 0:
            end = str(pd.Timestamp(end) + pd.Timedelta("1D"))
        elif end.strip().find(":") < 0:
            end = str(pd.Timestamp(end) + pd.Timedelta("1H"))
        else:
            end = str(pd.Timestamp(end) + pd.Timedelta("1M"))

        data = data \
            .reset_index() \
            .assign(Date_Time=lambda x: pd.to_datetime(x['Date'])) \
            .set_index("Date_Time") \
            .stack() \
            .rename_axis(index=["Date_Time", "ID"]) \
            .astype(dtype={"Open": np.float64, "High": np.float64, "Low": np.float64, "Close": np.float64, "Volume": np.float64}) \
            [["Open", "High", "Low", "Close", "Volume"]]

        if debug:
            print(data)

        if not hasattr(data.index.get_level_values("Date_Time"), "tz") or data.index.get_level_values("Date_Time").tz is None:
            data = ppl.localize(data, self.tz, self.tz)

        return data

# TODO : Turn this into getOptions()


class MIOptions:

    def __init__(self, symbol):

        optionsX = pd.DataFrame()
        stocklist = [symbol]
        for x in stocklist:
            tk = yf.Ticker(x)
            exps = tk.options  # expiration dates
            try:
                underlying = yf.download(tickers=symbol, period="1d", interval="1d", prepost=False)["Close"].values[0]

                for e in exps:
                    print(e)
                    opt = tk.option_chain(e)
                    opt.calls["type"] = "call"
                    opt.puts["type"] = "put"
                    opt = pd.DataFrame().append(opt.calls).append(opt.puts)
                    opt['expiry'] = datetime.strptime(e + " 21:00:00", "%Y-%m-%d %H:%M:%S")
                    opt['ticker'] = x
                    opt["underlying"] = underlying
                    optionsX = optionsX.append(opt, ignore_index=True)
            except(e):
                print("Error " + e)

        self.option = optionsX

    def get_all_data(self):
        return self.option
