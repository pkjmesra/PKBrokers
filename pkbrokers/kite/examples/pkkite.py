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

import argparse
import logging
import os
import sys
import threading
from queue import Queue

from PKDevTools.classes import log

# Argument Parsing for test purpose
argParser = argparse.ArgumentParser()
argParser.add_argument(
    "--auth",
    action="store_true",
    help="Authenticate with Zerodha's Kite with your username/password/totp and view/save access_token.",
    required=False,
)
argParser.add_argument(
    "--ticks",
    action="store_true",
    help="View ticks from Kite for all NSE Stocks.",
    required=False,
)
argParser.add_argument(
    "--history",
    action="store_true",
    help="Get history data for all NSE stocks.",
    required=False,
)
argParser.add_argument(
    "--instruments",
    action="store_true",
    help="Get instrument tokens for all NSE stocks.",
    required=False,
)
argParser.add_argument(
    "--pickle",
    action="store_true",
    help="Get instrument data from remote database and save into pickle for all NSE stocks.",
    required=False,
)
argsv = argParser.parse_known_args()
args = argsv[0]
LOG_LEVEL = logging.INFO
_watcher_queue = None


def validate_credentials():
    if not os.path.exists(".env.dev"):
        print(
            f"You need to have an .env.dev file in the root directory:\n{os.getcwd()}\nYou should save your Kite username in KUSER, your Kite password in KPWD and your Kite TOTP hash in KTOTP.\nYou can save the access_token in KTOKEN after authenticating here, but leave it blank for now.\nSee help for enabling TOTP: https://tinyurl.com/pkbrokers-totp \n.env.dev file should be in the following format with values:\nKTOKEN=\nKUSER=\nKPWD=\nKTOTP=\n"
        )
        print("\nPress any key to exit...")
        return False
    return True


def _process_ticks():
    from datetime import datetime

    from pkbrokers.kite.ticks import Tick

    global _watcher_queue
    while _watcher_queue is not None or (
        _watcher_queue is not None and not _watcher_queue.empty()
    ):
        try:
            tick = _watcher_queue.get(timeout=1)

            if tick is None:
                continue

            # Process the tick based on its type
            if isinstance(tick, Tick):
                # Convert to optimized format
                processed = {
                    "instrument_token": tick.instrument_token,
                    "timestamp": str(datetime.fromtimestamp(tick.exchange_timestamp)),
                    "last_price": tick.last_price if tick.last_price is not None else 0,
                    "day_volume": tick.day_volume if tick.day_volume is not None else 0,
                    "oi": tick.oi if tick.oi is not None else 0,
                    "buy_quantity": tick.buy_quantity
                    if tick.buy_quantity is not None
                    else 0,
                    "sell_quantity": tick.sell_quantity
                    if tick.sell_quantity is not None
                    else 0,
                    "high_price": tick.high_price if tick.high_price is not None else 0,
                    "low_price": tick.low_price if tick.low_price is not None else 0,
                    "open_price": tick.open_price if tick.open_price is not None else 0,
                    "prev_day_close": tick.prev_day_close
                    if tick.prev_day_close is not None
                    else 0,
                }
                print(processed)
                _watcher_queue.task_done()

        except KeyboardInterrupt:
            _watcher_queue = None
            sys.exit(0)
        except Exception as e:
            print(e)
            pass
    print("Exiting ...")


def kite_ticks():
    from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher

    global _watcher_queue
    _watcher_queue = Queue(maxsize=10000)
    watcher = KiteTokenWatcher(watcher_queue=_watcher_queue)
    print(
        "We're now ready to begin listening to ticks from Zerodha's Kite\nPress any key to continue..."
    )
    # Start processing thread (still needs to be thread)
    threading.Thread(target=_process_ticks, daemon=True).start()
    watcher.watch()


def kite_auth():
    # Configuration - load from environment in production
    from dotenv import dotenv_values

    from pkbrokers.kite.authenticator import KiteAuthenticator

    local_secrets = dotenv_values(".env.dev")
    credentials = {
        "api_key": "kitefront",
        "username": os.environ.get(
            "KUSER", local_secrets.get("KUSER", "You need your Kite username")
        ),
        "password": os.environ.get(
            "KPWD", local_secrets.get("KPWD", "You need your Kite password")
        ),
        "totp": os.environ.get(
            "KTOTP", local_secrets.get("KTOTP", "You need your Kite TOTP")
        ),
    }
    authenticator = KiteAuthenticator(timeout=10)
    req_token = authenticator.get_enctoken(**credentials)
    print(req_token)


def kite_history():
    from pkbrokers.kite.authenticator import KiteAuthenticator
    from pkbrokers.kite.instrumentHistory import KiteTickerHistory
    from pkbrokers.kite.instruments import KiteInstruments

    authenticator = KiteAuthenticator()
    enctoken = authenticator.get_enctoken()
    instruments = KiteInstruments(api_key="kitefront", access_token=enctoken)
    tokens = instruments.get_or_fetch_instrument_tokens(all_columns=True)
    # Create history client with the full response object
    history = KiteTickerHistory(
        enctoken=enctoken, access_token_response=authenticator.access_token_response
    )

    history.get_multiple_instruments_history(
        instruments=tokens, interval="day", forceFetch=True, insertOnly=True
    )
    if len(history.failed_tokens) > 0:
        history.get_multiple_instruments_history(
            instruments=history.failed_tokens,
            interval="day",
            forceFetch=True,
            insertOnly=True,
        )


def kite_instruments():
    from pkbrokers.kite.authenticator import KiteAuthenticator
    from pkbrokers.kite.instruments import KiteInstruments

    authenticator = KiteAuthenticator()
    enctoken = authenticator.get_enctoken()
    instruments = KiteInstruments(api_key="kitefront", access_token=enctoken)
    instruments.get_or_fetch_instrument_tokens(all_columns=True)


def kite_fetch_save_pickle():
    from pkbrokers.kite.datamanager import InstrumentDataManager

    manager = InstrumentDataManager()
    success = manager.execute()

    if success:
        # Example: Get data for a specific symbol
        symbol_data = manager.get_data_for_symbol("RELIANCE")
        if symbol_data:
            print(f"Found data for RELIANCE: {len(symbol_data)} days")
        else:
            print("No data found for RELIANCE")
    else:
        print("Failed to load or create instrument data")


def setupLogger(logLevel=LOG_LEVEL):
    log.setup_custom_logger(
        "pkbrokers",
        logLevel,
        trace=False,
        log_file_path="PKBrokers-log.txt",
        filter=None,
    )
    os.environ["PKDevTools_Default_Log_Level"] = str(logLevel)


def pkkite():
    if not validate_credentials():
        sys.exit()

    if args.auth:
        setupLogger()
        kite_auth()

    if args.ticks:
        setupLogger()
        kite_ticks()

    if args.history:
        setupLogger()
        kite_history()

    if args.instruments:
        setupLogger()
        kite_instruments()

    if args.pickle:
        setupLogger()
        kite_fetch_save_pickle()

    print(
        "You can use like this :\npkkite --auth\npkkite --ticks\npkkite --history\npkkite --instruments"
    )


if __name__ == "__main__":
    pkkite()
