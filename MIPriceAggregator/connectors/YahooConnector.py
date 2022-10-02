import yfinance as yf
import quantutils.dataset.pipeline as ppl


class YahooConnector:

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

    def getData(self, market, source, start="1979-01-01", end="2050-01-01", records=0):

        # set stock ticker symbol
        options = self.options

        # get daily stock prices over date range
        #json_prices = YahooFinancials([stock_symbol])
        #    .get_historical_price_data(options["start"], options["end"], options["interval"])

        data = yf.download(tickers=source["name"], start=start, end=end, interval=options["interval"], prepost=False)
        if (data.index.tz is None):
            data = ppl.localize(data, self.tz, "UTC")
        data.index.name = "Date_Time"

        return data[["Open", "High", "Low", "Close"]]
