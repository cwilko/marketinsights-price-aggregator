import requests
import pandas as pd
import numpy as np
import quantutils.dataset.pipeline as ppl
from datetime import datetime, date, timedelta
from urllib.parse import unquote
import time
from random import randint


class BarChartConnector:

    def __init__(self, dsName, tz, opts):
        self.dsName = dsName
        self.opts = opts
        self.tz = tz

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
            # print(response.json())
            return response.json()
        except e:
            print("Error: " + e)
            print(url)
            print(params)
        return None

    def getData(self, market, source, start="1979-01-01", end="2050-01-01", records=200):

        resp = self.get_api_data(self.historicalUrl, self.construct_quotes_payload(source["name"], count=records))

        if resp["count"] == 0:
            return None

        data = resp["data"]
        if data is not None:
            df = pd.DataFrame.from_records([quote["raw"] for quote in data]) \
                .replace('NA', np.nan) \
                .replace(np.nan, 0) \
                .apply(pd.to_numeric, errors='ignore') \
                .assign(tradeTime=lambda x: pd.to_datetime(x['tradeTime']))
            df.columns = ["Date_Time", "Open", "High", "Low", "Close", "Volume", "OpenInterest"]
            df = df.astype(dtype={"Open": "float64", "High": "float64", "Low": "float64", "Close": "float64", "Volume": "int64", "OpenInterest": "int64"})
            df = df.set_index("Date_Time")

        if (df.index.tz is None):
            data = ppl.localize(df, self.tz, self.tz)

        return df

    def getOptions(self, chain, appendUnderlying=True, start="1979-01-01", end="2050-01-01"):

        resp = self.get_api_data(self.apiUrl, self.construct_options_payload(chain["name"]))

        if resp["count"] == 0:
            return None

        data = resp["data"]
        if data is not None:
            data = pd.DataFrame.from_records([quote["raw"] for quote in data]) \
                .replace('NA', np.nan) \
                .replace(np.nan, 0) \
                .apply(pd.to_numeric, errors='ignore') \
                .assign(optionType=lambda x: [t.lower()[0] for t in x["optionType"]]) \
                .assign(Date_Time=date.today())
            data.columns = ["ID", "instrumentName", "type", "strike", "Open", "High", "Low", "Close", "Volume", "OpenInterest", "Date_Time"]
            data = data.astype(dtype={"Open": "float64", "High": "float64", "Low": "float64", "Close": "float64", "Volume": "int64", "OpenInterest": "int64"})
        data["underlying"] = chain["underlying"]
        underlyingPrice = np.nan
        if appendUnderlying:
            underlyingData = self.get_api_data(self.apiUrl, self.construct_quotes_payload(chain["underlying"], count=1))
            underlyingPrice = underlyingData[-1]["raw"]["lastPrice"]
        data["underlyingPrice"] = underlyingPrice

        data["ask"] = data["Close"]
        data["bid"] = data["Close"]
        data["expiry"] = datetime.strptime(chain["expiry"], '%Y-%m-%d')

        data = data.reset_index().set_index(["Date_Time", "ID"])
        if (data.index.get_level_values("Date_Time").tz is None):
            data = ppl.localize(data, self.tz, self.tz)
        data = data.sort_values("strike")

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
            data.columns = ["name", "instrumentName", "expiry", "underlying"]
        return data

    def getOptionInfo(self, chain):

        infoUrl = "https://www.barchart.com/symbols/{}/modules/dashboard?symbolType=2&symbolCode=FUT&hasOptions=0"
        return self.get_api_data(infoUrl.format(chain))["dashboard-commodity-profile"]["data"][0]["raw"]
