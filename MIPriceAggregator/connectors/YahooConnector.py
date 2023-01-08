import yfinance as yf
import quantutils.dataset.pipeline as ppl
from MIPriceAggregator.connectors.Connector import Connector


class YahooConnector(Connector):

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

    def getData(self, market, source, start, end, records, debug):

        # set stock ticker symbol
        options = self.options

        # get daily stock prices over date range
        #json_prices = YahooFinancials([stock_symbol])
        #    .get_historical_price_data(options["start"], options["end"], options["interval"])

        data = yf.download(tickers=source["ID"], start=start, end=end, interval=options["interval"], prepost=False) \
            .assign(ID=source["ID"]) \
            .reset_index() \
            .rename(columns={"Date": "Date_Time"}) \
            .set_index(["Date_Time", "ID"]) \
            .astype(dtype={"Open": "Float64", "High": "Float64", "Low": "Float64", "Close": "Float64", "Volume": "Float64"}) \
            [["Open", "High", "Low", "Close", "Volume"]]

        if (data.index.get_level_values("Date_Time").tz is None):
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
