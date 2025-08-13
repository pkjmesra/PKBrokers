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

import os

from dotenv import dotenv_values

from pkbrokers.kite.instruments import KiteInstruments
from pkbrokers.kite.zerodhaWebSocketClient import ZerodhaWebSocketClient

# Optimal batch size depends on your tick frequency
OPTIMAL_TOKEN_BATCH_SIZE = 500  # Zerodha allows max 500 instruments in one batch
NIFTY_50 = [256265]
BSE_SENSEX = [265]
OTHER_INDICES = [
    264969,
    263433,
    260105,
    257545,
    261641,
    262921,
    257801,
    261897,
    261385,
    259849,
    263945,
    263689,
    262409,
    261129,
    263177,
    260873,
    256777,
    266249,
    289545,
    274185,
    274441,
    275977,
    278793,
    279305,
    291593,
    289801,
    281353,
    281865,
]


class KiteTokenWatcher:
    def __init__(self, tokens=[], watcher_queue=None, client=None):
        self.watcher_queue = watcher_queue
        # Split into batches of OPTIMAL_TOKEN_BATCH_SIZE (Zerodha's recommended chunk size)
        self.token_batches = [
            tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
            for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
        ]
        self.client = client

    def watch(self):
        local_secrets = dotenv_values(".env.dev")
        if len(self.token_batches) == 0:
            API_KEY = "kitefront"
            ACCESS_TOKEN = (
                os.environ.get(
                    "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
                ),
            )
            kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)
            equities_count = kite.get_instrument_count()
            if equities_count == 0:
                kite.sync_instruments(force_fetch=True)
            equities = kite.get_equities(column_names="instrument_token")
            tokens = kite.get_instrument_tokens(equities=equities)
            tokens = NIFTY_50 + BSE_SENSEX + tokens
            self.token_batches = [
                tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
                for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
            ]

        if self.client is None:
            self.client = ZerodhaWebSocketClient(
                enctoken=os.environ.get(
                    "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
                ),
                user_id=os.environ.get(
                    "KUSER", local_secrets.get("KUSER", "You need your Kite user")
                ),
                token_batches=self.token_batches,
                watcher_queue=self.watcher_queue,
            )

        try:
            self.client.start()
        except KeyboardInterrupt:
            self.client.stop()


"""
# Example usage
if __name__ == "__main__":
    from pkbrokers.kite.instruments import KiteInstruments
    from dotenv import dotenv_values
    local_secrets = dotenv_values(".env.dev")
    API_KEY = "kitefront"
    ACCESS_TOKEN = os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token")),
    kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)
    tokens = kite.get_instrument_tokens(equities=kite.get_equities(column_names='instrument_token'))
    # Load instrument tokens from CSV/API
    # with open('instruments.csv') as f:
    #     instruments = pd.read_csv(f)
    #     tokens = instruments['instrument_token'].tolist()

    # Split into batches of 500 (Zerodha's recommended chunk size)
    token_batches = [tokens[i:i+500] for i in range(0, len(tokens), 500)]

    from dotenv import dotenv_values
    local_secrets = dotenv_values(".env.dev")

    client = ZerodhaWebSocketClient(
        enctoken=os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token")),
        user_id=os.environ.get("KUSER",local_secrets.get("KUSER","You need your Kite user"))
    )

    try:
        client.start()
    except KeyboardInterrupt:
        client.stop()


# Ensure your access_token (enctoken) is fresh (they expire daily)
# To get a fresh access token:

# from kiteconnect import KiteConnect
# kite = KiteConnect(api_key="your_api_key")
# print(kite.generate_session("request_token", "your_api_secret"))

# Testing Steps:
# First verify you can connect using the KiteConnect API
# Ensure your token was generated recently
# Try with just 1-2 instrument tokens initially

# Enable full debug logging:

# logger = logging.getLogger('websockets')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

# Verify token manually:

# import requests
# response = requests.get(
#     "https://api.kite.trade/user/profile",
#     headers={"Authorization": f"enctoken {your_access_token}"}
# )
# print(response.status_code, response.text)

# Check token expiration:

# from datetime import datetime
# token_time = datetime.fromtimestamp(int(your_access_token.split('.')[0]))
# print(f"Token was generated at: {token_time}")

import os
if __name__ == "__main__":
    from PKDevTools.classes import log
    log.setup_custom_logger(
        "pkscreener",
        log.logging.INFO,
        trace=False,
        log_file_path="PKBrokers-log.txt",
        filter=None,
    )
    os.environ["PKDevTools_Default_Log_Level"] = str(log.logging.INFO)
    from pkbrokers.kite.ticks import KiteTokenWatcher
    watcher = KiteTokenWatcher()
    watcher.watch()
"""
