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
import sys
import argparse
import logging

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
    help="Get history data for all NSE stocks.",
    required=False,
)
argsv = argParser.parse_known_args()
args = argsv[0]
LOG_LEVEL=logging.INFO

if __name__ == "__main__":

    def validate_credentials():
        if not os.path.exists(".env.dev"):
            print(f"You need to have an .env.dev file in the root directory:\n{os.getcwd()}\nYou should save your Kite username in KUSER, your Kite password in KPWD and your Kite TOTP hash in KTOTP.\nYou can save the access_token in KTOKEN after authenticating here, but leave it blank for now.\nSee help for enabling TOTP: https://tinyurl.com/pkbrokers-totp \n.env.dev file should be in the following format with values:\nKTOKEN=\nKUSER=\nKPWD=\nKTOTP=\n")
            print("\nPress any key to exit...")
            return False
        return True

    def kite_ticks():
        from pkbrokers.kite.ticks import KiteTokenWatcher
        watcher = KiteTokenWatcher()
        print("We're now ready to begin listening to ticks from Zerodha's Kite\nPress any key to continue...")
        watcher.watch()
    
    def kite_auth():
        # Configuration - load from environment in production
        from dotenv import dotenv_values
        from pkbrokers.kite.authenticator import KiteAuthenticator
        local_secrets = dotenv_values(".env.dev")
        credentials = {
                        "api_key" : "kitefront",
                        "username" : os.environ.get("KUSER",local_secrets.get("KUSER","You need your Kite username")),
                        "password" : os.environ.get("KPWD",local_secrets.get("KPWD","You need your Kite password")),
                        "totp" : os.environ.get("KTOTP",local_secrets.get("KTOTP","You need your Kite TOTP")),
                    }
        authenticator = KiteAuthenticator(timeout=10)
        req_token = authenticator.get_enctoken(**credentials)
        print(req_token)
    
    def kite_history():
        print("History data goes here.")

    def pkkite():
        if not validate_credentials():
            sys.exit()
        from PKDevTools.classes import log
        log.setup_custom_logger(
            "pkbrokers",
            LOG_LEVEL,
            trace=False,
            log_file_path="PKBrokers-log.txt",
            filter=None,
        )
        os.environ["PKDevTools_Default_Log_Level"] = str(LOG_LEVEL)
        if args.auth:
            kite_auth()

        if args.ticks:
            kite_ticks()

    pkkite()
