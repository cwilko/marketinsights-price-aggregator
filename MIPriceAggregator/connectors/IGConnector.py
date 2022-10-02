from datetime import datetime, date
from trading_ig.rest import IGService, ApiExceededException
from tenacity import Retrying, wait_exponential, retry_if_exception_type
import quantutils.dataset.pipeline as ppl
import pandas as pd
from dateutil import parser


class IGConnector:

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

        retryer = Retrying(wait=wait_exponential(), retry=retry_if_exception_type(ApiExceededException))
        self.ig_service = IGService(options["IG_user"], options["IG_pw"], options["IG_apikey"], "DEMO", options["IG_account_no"], retryer=retryer)
        self.ig_service.create_session(version='3')

    def getData(self, market, source, start="1979-01-01", end="2050-01-01", records=0):

        options = self.options

        start = str(datetime.strptime(start, '%Y-%m-%d'))
        end = str(datetime.strptime(end, '%Y-%m-%d'))

        result = self.ig_service.fetch_historical_prices_by_epic_and_date_range(epic=source["name"], resolution=options["interval"], start_date=start, end_date=end)
        data = result["prices"]["bid"]

        if (data.index.tz is None):
            data = ppl.localize(data, self.tz, self.tz)
        data.index.name = "Date_Time"

        return data[["Open", "High", "Low", "Close"]]

    def getOptions(self, chain, appendUnderlying=True, start="1979-01-01", end="2050-01-01"):

        # Options menu id = 195913
        optionData = self.ig_service.fetch_sub_nodes_by_node("195913")["nodes"]
        option = optionData[optionData["name"] == chain["name"]]
        optionId = option["id"].values[0]

        # Fetch strike prices
        optionStrikes = self.ig_service.fetch_sub_nodes_by_node(optionId)["nodes"]
        data = pd.DataFrame()
        for _, strikeRange in optionStrikes.iterrows():
            optionDetails = self.ig_service.fetch_sub_nodes_by_node(strikeRange["id"])["markets"]
            optionDetails = optionDetails.set_index("epic")
            data = ppl.merge(data, optionDetails)

        underlyingEpic = self.ig_service.fetch_market_by_epic(chain["underlying"]).snapshot
        underlyingPrice = (underlyingEpic.bid + underlyingEpic.offer) / 2.0

        data["type"] = [x.split()[-1][0].lower() for x in data["instrumentName"].values]
        data["strike"] = [int(x.split()[-2]) for x in data["instrumentName"].values]
        data["ask"] = data["offer"]

        exp = self.ig_service.fetch_market_by_epic(data.index.values[0]).instrument.expiryDetails.lastDealingDate
        data["expiry"] = parser.parse(exp)
        data["underlying"] = underlyingPrice
        data["Date_Time"] = date.today()

        data = data.reset_index().set_index(["Date_Time", "epic"])
        data = data.sort_values("strike")

        return data[["instrumentName", "type", "strike", "expiry", "underlying", "ask", "bid"]]
