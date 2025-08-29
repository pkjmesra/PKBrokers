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
import multiprocessing
import os
import sys

from PKDevTools.classes import log

LOG_LEVEL = (
    logging.INFO
    if "PKDevTools_Default_Log_Level" not in os.environ.keys()
    else int(os.environ["PKDevTools_Default_Log_Level"])
)

if __name__ == "__main__":
    multiprocessing.freeze_support()

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
    # action="store_true",
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
try:
    argsv = argParser.parse_known_args()
except BaseException:
    pass

args = argsv[0]


def validate_credentials():
    if not os.path.exists(".env.dev"):
        print(
            f"You need to have an .env.dev file in the root directory:\n{os.getcwd()}\nYou should save your Kite username in KUSER, your Kite password in KPWD and your Kite TOTP hash in KTOTP.\nYou can save the access_token in KTOKEN after authenticating here, but leave it blank for now.\nSee help for enabling TOTP: https://tinyurl.com/pkbrokers-totp \n.env.dev file should be in the following format with values:\nKTOKEN=\nKUSER=\nKPWD=\nKTOTP=\n"
        )
        print("\nPress any key to exit...")
        return False
    return True


def kite_ticks():
    from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher

    watcher = KiteTokenWatcher()
    print("We're now ready to begin listening to ticks from Zerodha's Kite...")
    # Start watching for ticks
    try:
        watcher.watch()
    except KeyboardInterrupt:
        watcher.stop()
    except Exception as e:
        print(f"Error: {e}")
        watcher.stop()


def kite_auth():
    # Configuration - load from environment in production
    from PKDevTools.classes.Environment import PKEnvironment

    from pkbrokers.kite.authenticator import KiteAuthenticator

    local_secrets = PKEnvironment().allSecrets
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
    authenticator.get_enctoken(**credentials)
    # print(req_token)


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
        instruments=tokens, interval=args.history, forceFetch=True, insertOnly=True
    )
    if len(history.failed_tokens) > 0:
        history.get_multiple_instruments_history(
            instruments=history.failed_tokens,
            interval=args.history,
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
        print("Saved instrument data into the pickle file")
    else:
        print("Failed to load or create instrument data")


def setupLogger(logLevel=LOG_LEVEL):
    os.environ["PKDevTools_Default_Log_Level"] = str(logLevel)
    log.setup_custom_logger(
        "pkbrokers",
        logLevel,
        trace=False,
        log_file_path="PKBrokers-log.txt",
        filter=None,
    )


def pkkite():
    if sys.platform.startswith("darwin"):
        try:
            multiprocessing.set_start_method("spawn" if sys.platform.startswith("darwin") else "spawn", force=True)
        except RuntimeError:  # pragma: no cover
            pass

    if not validate_credentials():
        sys.exit()

    if args.auth:
        setupLogger()
        kite_auth()

    if args.ticks:
        setupLogger()
        kite_auth()
        kite_ticks()

    if args.history:
        from pkbrokers.kite.instrumentHistory import Historical_Interval

        supported_intervals = [member.value for member in Historical_Interval]
        if args.history not in supported_intervals:
            intervals = ', '.join(map(lambda x: x.value, Historical_Interval))
            example_lines = '\n'.join(map(lambda x: f"--history={x.value}", Historical_Interval))
            print(f"--history= requires at least one of the following parameters: {intervals}\nFor example:\n{example_lines}")
        else:
            setupLogger()
            kite_auth()
            kite_history()

    if args.instruments:
        setupLogger()
        kite_auth()
        kite_instruments()

    if args.pickle:
        setupLogger()
        kite_auth()
        kite_fetch_save_pickle()

    print(
        "You can use like this :\npkkite --auth\npkkite --ticks\npkkite --history\npkkite --instruments\npkkite --pickle"
    )


if __name__ == "__main__":
    log_files = ["PKBrokers-log.txt", "PKBrokers-DBlog.txt"]
    for file in log_files:
        try:
            os.remove(file)
        except BaseException:
            pass
    pkkite()
