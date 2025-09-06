# -*- coding: utf-8 -*-
"""
The MIT License (MIT)

Copyright (c) 2023 pkjmesra

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

import dateutil
import dateutil.parser
import requests

# from kite_trade import *


def get_enctoken(userid, password, twofa):
    session = requests.Session()
    response = session.post(
        "https://kite.zerodha.com/api/login",
        data={"user_id": userid, "password": password},
    )
    response = session.post(
        "https://kite.zerodha.com/api/twofa",
        data={
            "request_id": response.json()["data"]["request_id"],
            "twofa_value": twofa,
            "user_id": response.json()["data"]["user_id"],
        },
    )
    enctoken = response.cookies.get("enctoken")
    if enctoken:
        return enctoken
    else:
        raise Exception("Enter valid details !!!!")


class KiteApp:
    # Products
    PRODUCT_MIS = "MIS"
    PRODUCT_CNC = "CNC"
    PRODUCT_NRML = "NRML"
    PRODUCT_CO = "CO"

    # Order types
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_SLM = "SL-M"
    ORDER_TYPE_SL = "SL"

    # Varities
    VARIETY_REGULAR = "regular"
    VARIETY_CO = "co"
    VARIETY_AMO = "amo"

    # Transaction type
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"

    # Validity
    VALIDITY_DAY = "DAY"
    VALIDITY_IOC = "IOC"

    # Exchanges
    EXCHANGE_NSE = "NSE"
    EXCHANGE_BSE = "BSE"
    EXCHANGE_NFO = "NFO"
    EXCHANGE_CDS = "CDS"
    EXCHANGE_BFO = "BFO"
    EXCHANGE_MCX = "MCX"

    def __init__(self, enctoken):
        self.enctoken = enctoken
        self.headers = {"Authorization": f"enctoken {self.enctoken}"}
        self.session = requests.session()
        self.root_url = "https://kite.zerodha.com/oms"
        self.session.get(self.root_url, headers=self.headers)

    def instruments(self, exchange=None):
        data = self.session.get("https://api.kite.trade/instruments").text.split("\n")
        Exchange = []
        for i in data[1:-1]:
            row = i.split(",")
            if exchange is None or exchange == row[11]:
                Exchange.append(
                    {
                        "instrument_token": int(row[0]),
                        "exchange_token": row[1],
                        "tradingsymbol": row[2],
                        "name": row[3][1:-1],
                        "last_price": float(row[4]),
                        "expiry": dateutil.parser.parse(row[5]).date()
                        if row[5] != ""
                        else None,
                        "strike": float(row[6]),
                        "tick_size": float(row[7]),
                        "lot_size": int(row[8]),
                        "instrument_type": row[9],
                        "segment": row[10],
                        "exchange": row[11],
                    }
                )
        return Exchange

    def historical_data(
        self, instrument_token, from_date, to_date, interval, continuous=False, oi=False
    ):
        params = {
            "from": from_date,
            "to": to_date,
            "interval": interval,
            "continuous": 1 if continuous else 0,
            "oi": 1 if oi else 0,
        }
        lst = self.session.get(
            f"{self.root_url}/instruments/historical/{instrument_token}/{interval}",
            params=params,
            headers=self.headers,
        ).json()["data"]["candles"]
        records = []
        for i in lst:
            record = {
                "date": dateutil.parser.parse(i[0]),
                "open": i[1],
                "high": i[2],
                "low": i[3],
                "close": i[4],
                "volume": i[5],
            }
            if len(i) == 7:
                record["oi"] = i[6]
            records.append(record)
        return records

    def margins(self):
        margins = self.session.get(
            f"{self.root_url}/user/margins", headers=self.headers
        ).json()["data"]
        return margins

    def profile(self):
        profile = self.session.get(
            f"{self.root_url}/user/profile", headers=self.headers
        ).json()["data"]
        return profile

    def orders(self):
        orders = self.session.get(
            f"{self.root_url}/orders", headers=self.headers
        ).json()["data"]
        return orders

    def positions(self):
        positions = self.session.get(
            f"{self.root_url}/portfolio/positions", headers=self.headers
        ).json()["data"]
        return positions

    def place_order(
        self,
        variety,
        exchange,
        tradingsymbol,
        transaction_type,
        quantity,
        product,
        order_type,
        price=None,
        validity=None,
        disclosed_quantity=None,
        trigger_price=None,
        squareoff=None,
        stoploss=None,
        trailing_stoploss=None,
        tag=None,
    ):
        params = locals()
        del params["self"]
        for k in list(params.keys()):
            if params[k] is None:
                del params[k]
        order_id = self.session.post(
            f"{self.root_url}/orders/{variety}", data=params, headers=self.headers
        ).json()["data"]["order_id"]
        return order_id

    def modify_order(
        self,
        variety,
        order_id,
        parent_order_id=None,
        quantity=None,
        price=None,
        order_type=None,
        trigger_price=None,
        validity=None,
        disclosed_quantity=None,
    ):
        params = locals()
        del params["self"]
        for k in list(params.keys()):
            if params[k] is None:
                del params[k]

        order_id = self.session.put(
            f"{self.root_url}/orders/{variety}/{order_id}",
            data=params,
            headers=self.headers,
        ).json()["data"]["order_id"]
        return order_id

    def cancel_order(self, variety, order_id, parent_order_id=None):
        order_id = self.session.delete(
            f"{self.root_url}/orders/{variety}/{order_id}",
            data={"parent_order_id": parent_order_id} if parent_order_id else {},
            headers=self.headers,
        ).json()["data"]["order_id"]
        return order_id


# # Place Order
# order = kite.place_order(variety=kite.VARIETY_REGULAR,
#                          exchange=kite.EXCHANGE_NSE,
#                          tradingsymbol="ACC",
#                          transaction_type=kite.TRANSACTION_TYPE_BUY,
#                          quantity=1,
#                          product=kite.PRODUCT_MIS,
#                          order_type=kite.ORDER_TYPE_MARKET,
#                          price=None,
#                          validity=None,
#                          disclosed_quantity=None,
#                          trigger_price=None,
#                          squareoff=None,
#                          stoploss=None,
#                          trailing_stoploss=None,
#                          tag="AlgoTrader")

# print(order)

# # Modify Order
# kite.modify_order(variety=kite.VARIETY_REGULAR,
#                   order_id="order_id",
#                   parent_order_id=None,
#                   quantity=5,
#                   price=200,
#                   order_type=kite.ORDER_TYPE_LIMIT,
#                   trigger_price=None,
#                   validity=kite.VALIDITY_DAY,
#                   disclosed_quantity=None)

# # Cancel order
# kite.cancel_order(variety=kite.VARIETY_REGULAR,
#                   order_id="order_id",
#                   parent_order_id=None)

# # Get Historical Data

# import datetime
# instrument_token = 9604354
# from_datetime = datetime.datetime.now() - datetime.timedelta(days=7)     # From last & days
# to_datetime = datetime.datetime.now()
# interval = "5minute"
# print(kite.historical_data(instrument_token, from_datetime, to_datetime, interval, continuous=False, oi=False))

# # Get Tick Data 'Use Websocket'

# from kiteconnect import KiteTicker
# user_id = kite.profile()["user_id"]
# kws = KiteTicker(api_key="AlgoTrader", access_token=enctoken+"&user_id="+user_id)

# def on_ticks(ws, ticks):
#     print(ticks)

# kws.on_ticks = on_ticks
# kws.connect(threaded=True)
# while not kws.is_connected():
#     time.sleep(1)
# print("WebSocket : Connected")
# # use instrument_token to get data
# kws.subscribe([256265, 260105, 738561, 5633])
# kws.set_mode(kws.MODE_QUOTE, [256265, 260105, 738561, 5633])
# time.sleep(30)
# kws.unsubscribe([256265, 260105, 738561, 5633])

# # Basic API Calls

# # Basic calls
# print(kite.profile())
# print(kite.margins())
# print(kite.orders())
# print(kite.positions())

# # Get instrument or exchange
# print(kite.instruments())
# print(kite.instruments("NSE"))
# print(kite.instruments("NFO"))

# # Log In Method

# # # Second way is provide 'enctoken' manually from 'kite.zerodha.com' website
# # # Than you can use login window of 'kite.zerodha.com' website Just don't logout from that window
# # # # Process shared on YouTube 'TradeViaPython'

# enctoken = ""
# kite = KiteApp(enctoken=enctoken)

# # Log In Method

# # # First Way to Login
# # # You can use your Kite app in mobile
# # # But You can't login anywhere in 'kite.zerodha.com' website else this session will disconnected

# user_id = ""       # Login Id
# password = ""      # Login password
# twofa = ""         # Login Pin or TOTP

# enctoken = get_enctoken(user_id, password, twofa)
# kite = KiteApp(enctoken=enctoken)
