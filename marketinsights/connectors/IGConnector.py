from marketinsights.connectors.Connector import Connector
from datetime import datetime, date
from trading_ig.rest import IGService, ApiExceededException
from tenacity import Retrying, wait_exponential, retry_if_exception_type
import quantutils.dataset.pipeline as ppl
import pandas as pd
import numpy as np
from dateutil import parser


class IGConnector(Connector):

    def __init__(self, dsName, tz, options):
        self.dsName = dsName
        self.options = options
        self.tz = tz

        retryer = Retrying(wait=wait_exponential(), retry=retry_if_exception_type(ApiExceededException))
        self.ig_service = IGService(options["IG_user"], options["IG_pw"], options["IG_apikey"], "DEMO", options["IG_account_no"], retryer=retryer)
        self.ig_service.create_session(version='3')

    def getSourceData(self, market, source, start, end, records, debug):

        options = self.options

        start = str(datetime.strptime(start, '%Y-%m-%d'))
        end = str(datetime.strptime(end, '%Y-%m-%d'))

        result = self.ig_service.fetch_historical_prices_by_epic_and_date_range(epic=source["ID"], resolution=options["interval"], start_date=start, end_date=end)
        data = result["prices"]["bid"] \
            .assign(ID=source["ID"]) \
            .reset_index() \
            .rename(columns={"DateTime": "Date_Time"}) \
            .set_index(["Date_Time", "ID"]) \
            .astype(dtype={"Open": np.float64, "High": np.float64, "Low": np.float64, "Close": np.float64}) \
            [["Open", "High", "Low", "Close"]]

        if (data.index.get_level_values("Date_Time").tz is None):
            data = ppl.localize(data, self.tz, self.tz)

        return data

    def getOptions(self, chain, start, end, records, debug):

        # Options menu id = 195913
        optionData = self.ig_service.fetch_sub_nodes_by_node("195913")["nodes"]
        option = optionData[optionData["name"] == chain["ID"]]
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

        data = data.reset_index() \
            .rename(columns={"epic": "ID"}) \
            .set_index(["Date_Time", "ID"])

        return data[["instrumentName", "type", "strike", "expiry", "underlying", "ask", "bid"]]
