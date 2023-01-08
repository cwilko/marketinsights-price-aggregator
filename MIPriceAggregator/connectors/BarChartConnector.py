from MIPriceAggregator.connectors.Connector import Connector
import requests
import pandas as pd
import numpy as np
import quantutils.dataset.pipeline as ppl
import quantutils.core.options as opt_utils
from datetime import datetime, date, timedelta
from urllib.parse import unquote
import time
from random import randint


class BarChartConnector(Connector):

    def __init__(self, dsName, tz, opts):
        self.dsName = dsName
        self.opts = opts
        self.tz = tz
        self.marketData = None

        # Random url to generate a cookie
        sessionUrl = r'https://www.barchart.com/futures/quotes/CLX22/options/nov-22'
        # API endpoint
        self.apiUrl = r'https://www.barchart.com/proxies/core-api/v1/quotes/get'
        self.historicalUrl = r'https://www.barchart.com/proxies/core-api/v1/historical/get'

        self.session = requests.Session()
        self.session.get(sessionUrl, params={'page': 'all'}, headers=self.construct_headers())

    def construct_headers(self, session=None):
        headers = {
            'accept': 'application/json',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'referer': 'https://www.barchart.com/futures/quotes/CLJ19/all-futures?page=all',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
        }
        if session is not None:
            headers['x-xsrf-token'] = unquote(unquote(session.cookies.get_dict()['XSRF-TOKEN']))
        return headers

    def construct_chains_payload(self, symbolRoot):
        return {
            'root': symbolRoot,
            'list': 'futures.contractInRoot',
            'fields': 'symbol,contractSymbol,daysToExpiration',
            'limit': '20',
            'hasOptions': 'true',
            'raw': '1',
            'page': '1'
        }

    def construct_options_payload(self, contractSymbol):
        return {
            'symbol': contractSymbol,
            'list': 'futures.options',
            'fields': 'longSymbol,symbolName,optionType,strikePrice,dailyOpenPrice,dailyHighPrice,dailyLowPrice,dailyLastPrice,dailyVolume,dailyOpenInterest',
            'meta': 'field.shortName',
            'hasOptions': 'true',
            'raw': '1'
        }

    def construct_quotes_payload(self, symbol, count=200):
        return {
            'symbol': symbol,
            'fields': 'tradeTime.format(d/m/Y),openPrice,highPrice,lowPrice,lastPrice,volume,openInterest',
            'limit': str(count),
            'type': 'eod',
            'meta': 'field.shortName,field.type,field.description',
            'orderBy': 'tradeTime',
            'orderDir': 'asc',
            'raw': '1'
        }

    def get_api_data(self, url, params=None, debug=False):

        time.sleep(randint(2, 5))
        if debug:
            print("Requesting from: " + url)

        try:
            response = self.session.get(
                url=url,
                params=params,
                headers=self.construct_headers(self.session)
            )
            if debug:
                print(response.json())
            return response.json()
        except e:
            print("Error: " + e)
            print(url)
            print(params)
        return None

    def setState(self, state):
        if "marketData" in state:
            self.marketData = state["marketData"]
            if self.marketData is not None:
                self.marketData = self.marketData.reset_index().set_index("Date_Time")

    def getData(self, market, source, start, end, records, debug):

        resp = self.get_api_data(self.historicalUrl, self.construct_quotes_payload(source["ID"], count=records), debug)

        if resp["count"] == 0:
            return None

        data = resp["data"]
        if data is not None:
            data = pd.DataFrame.from_records([quote["raw"] for quote in data]) \
                .replace('NA', np.nan) \
                .apply(pd.to_numeric, errors='ignore') \
                .assign(ID=source["ID"]) \
                .rename(columns={"tradeTime": "Date_Time"}) \
                .assign(Date_Time=lambda x: pd.to_datetime(x['Date_Time'])) \
                .set_index(["Date_Time", "ID"])

            data.columns = ["Open", "High", "Low", "Close", "Volume", "OpenInterest"]
            data = data.astype(dtype={"Open": "Float64", "High": "Float64", "Low": "Float64", "Close": "Float64", "Volume": "Float64", "OpenInterest": "Float64"})

        if (data.index.get_level_values("Date_Time").tz is None):
            data = ppl.localize(data, self.tz, self.tz)

        return data

    def getOptionData(self, chain, start, end, records, debug):

        if records == 1:
            return self.getOptionChain(chain, start, end, debug)

        optionData = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=[u'Date_Time', u'ID']))

        for option in chain["options"]:
            resp = self.get_api_data(self.historicalUrl, self.construct_quotes_payload(option["ID"], count=records), debug)

            if resp["count"] == 0:
                return None

            data = resp["data"]
            if data is not None:
                data = pd.DataFrame.from_records([quote["raw"] for quote in data]) \
                    .replace('NA', np.nan) \
                    .apply(pd.to_numeric, errors='ignore') \
                    .rename(columns={"tradeTime": "Date_Time", "openPrice": "Open", "highPrice": "High", "lowPrice": "Low", "lastPrice": "Close", "volume": "Volume", "openInterest": "OpenInterest"}) \
                    .assign(Date_Time=lambda x: pd.to_datetime(x['Date_Time'], utc=True)) \
                    .assign(instrumentName=option["instrumentName"]) \
                    .assign(expiry=datetime.strptime(chain["expiry"], '%Y-%m-%d')) \
                    .assign(expiry=lambda x: pd.to_datetime(x['expiry'], utc=True)) \
                    .assign(timeToExpiry=lambda x: [((exp.days * 24. * 3600. + exp.seconds + 3600) / (24. * 3600.)) for exp in (x['expiry'].sub(x['Date_Time']))]) \
                    .assign(bid=lambda x: x['Close']) \
                    .assign(ask=lambda x: x['Close']) \
                    .assign(underlyingSymbol=chain["underlying"]) \
                    .assign(underlying=lambda x: self.addUnderlyingValues(x, self.marketData, chain["underlying"])) \
                    .assign(strike=float(option["strike"])) \
                    .assign(type=option["type"]) \
                    .assign(IV=lambda x: opt_utils.get_IV(x)) \
                    .assign(ID=option["ID"]) \
                    .set_index(["Date_Time", "ID"])

                data = data.astype(dtype={"Open": "Float64", "High": "Float64", "Low": "Float64", "Close": "Float64", "Volume": "Float64", "OpenInterest": "Float64", "strike": "Float64"})

                if (data.index.get_level_values("Date_Time").tz is None):
                    data = ppl.localize(data, self.tz, self.tz)

                if debug:
                    print("Adding " + option["ID"])

                optionData = pd.concat([optionData, data])

        return optionData

    def getOptionChain(self, chain, start="1979-01-01", end="2050-01-01", debug=False):

        resp = self.get_api_data(self.apiUrl, self.construct_options_payload(chain["ID"]), debug)

        if resp["count"] == 0:
            return None

        data = resp["data"]
        if data is not None:
            data = pd.DataFrame.from_records([quote["raw"] for quote in data]) \
                .replace('NA', np.nan) \
                .apply(pd.to_numeric, errors='ignore') \
                .rename(columns={"dailyOpenPrice": "Open", "dailyHighPrice": "High", "dailyLowPrice": "Low", "dailyLastPrice": "Close", "dailyVolume": "Volume", "dailyOpenInterest": "OpenInterest", "strikePrice": "strike", "longSymbol": "ID", "symbolName": "instrumentName", "optionType": "type"}) \
                .assign(Date_Time=date.today()) \
                .assign(Date_Time=lambda x: pd.to_datetime(x['Date_Time'], utc=True)) \
                .assign(expiry=datetime.strptime(chain["expiry"], '%Y-%m-%d')) \
                .assign(expiry=lambda x: pd.to_datetime(x['expiry'], utc=True)) \
                .assign(timeToExpiry=lambda x: [((exp.days * 24. * 3600. + exp.seconds + 3600) / (24. * 3600.)) for exp in (x['expiry'].sub(x['Date_Time']))]) \
                .assign(bid=lambda x: x['Close']) \
                .assign(ask=lambda x: x['Close']) \
                .assign(underlyingSymbol=chain["underlying"]) \
                .assign(underlying=lambda x: self.addUnderlyingValues(x, self.marketData, chain["underlying"])) \
                .assign(type=lambda x: [t.lower()[0] for t in x["type"]]) \
                .assign(IV=lambda x: opt_utils.get_IV(x)) \
                .set_index(["Date_Time", "ID"])

            data = data.astype(dtype={"Open": "Float64", "High": "Float64", "Low": "Float64", "Close": "Float64", "Volume": "Float64", "OpenInterest": "Float64"})

            if (data.index.get_level_values("Date_Time").tz is None):
                data = ppl.localize(data, self.tz, self.tz)

        return data

    def getOptionChains(self, root):

        resp = self.get_api_data(self.apiUrl, self.construct_chains_payload(root))

        if resp["count"] == 0:
            return None

        data = resp["data"]
        if data is not None:
            today = date.today()
            data = pd.DataFrame.from_records([entry["raw"] for entry in data]) \
                .replace('NA', np.nan) \
                .assign(underlying=lambda x: x["symbol"]) \
                .apply(pd.to_numeric, errors='ignore')
            data.columns = ["ID", "instrumentName", "expiry", "underlying"]
        return data

    def getOptionChainInfo(self, chain):

        infoUrl = "https://www.barchart.com/symbols/{}/modules/dashboard?symbolType=2&symbolCode=FUT&hasOptions=0"
        return self.get_api_data(infoUrl.format(chain))["dashboard-commodity-profile"]["data"][0]["raw"]
