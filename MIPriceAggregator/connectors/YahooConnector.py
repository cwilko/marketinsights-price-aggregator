import yfinance as yf
import quantutils.dataset.pipeline as ppl


class YahooConnector:

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

    def getData(self, market, source, start="1979-01-01", end="2050-01-01", records=0, debug=False):

        # set stock ticker symbol
        options = self.options

        # get daily stock prices over date range
        #json_prices = YahooFinancials([stock_symbol])
        #    .get_historical_price_data(options["start"], options["end"], options["interval"])

        data = yf.download(tickers=source["name"], start=start, end=end, interval=options["interval"], prepost=False) \
            .assign(ID=source["name"]) \
            .reset_index() \
            .rename(columns={"Date": "Date_Time"}) \
            .set_index(["Date_Time", "ID"]) \
            [["Open", "High", "Low", "Close", "Volume"]]

        if (data.index.get_level_values("Date_Time").tz is None):
            data = ppl.localize(data, self.tz, self.tz)

        return data
