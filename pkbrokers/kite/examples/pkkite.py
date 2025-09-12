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
from PKDevTools.classes.log import default_logger

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
    "--consumer",
    action="store_true",
    help="Starts the consumer process and downloads the daily ticks.json file from PKTickBot",
    required=False,
)
argParser.add_argument(
    "--ticks",
    action="store_true",
    help="View ticks from Kite for all NSE Stocks.",
    required=False,
)
argParser.add_argument(
    "--token",
    action="store_true",
    help="View kite token",
    required=False,
)
argParser.add_argument(
    "--refresh_token",
    action="store_true",
    help="Refresh kite token",
    required=False,
)
argParser.add_argument(
    "--history",
    # action="store_true",
    help="Get history data for all NSE stocks.",
    required=False,
)
argParser.add_argument(
    "--pastoffset",
    # action="store_true",
    help="Number of days in past for fetching the data.",
    required=False,
)
argParser.add_argument(
    "--instruments",
    action="store_true",
    help="Get instrument tokens for all NSE stocks.",
    required=False,
)
argParser.add_argument(
    "--orchestrate",
    action="store_true",
    help="Orchestrate running the PKTickBot as well as. run the kite daily ticks processes.",
    required=False,
)

argParser.add_argument(
    "--pickle",
    action="store_true",
    help="Get instrument data from remote database and save into pickle for all NSE stocks.",
    required=False,
)
argParser.add_argument(
    "--test",
    action="store_true",
    help="Test various workflows in realtime.",
    required=False,
)
try:
    argsv = argParser.parse_known_args()
except BaseException as e:
    print(f"Could not parse arguments: Error: {e}")
    print(
        "You can use like this :\npkkite --auth\npkkite --ticks\npkkite --history\npkkite --instruments\npkkite --pickle\n\n"
    )
    from pkbrokers.kite.instrumentHistory import Historical_Interval

    intervals = ", ".join(map(lambda x: x.value, Historical_Interval))
    example_lines = "\n".join(
        map(lambda x: f"pkkite --history={x.value}", Historical_Interval)
    )
    print(
        f"--history= requires at least one of the following parameters: {intervals}\nFor example:\n{example_lines}"
    )
    sys.exit(0)

args = argsv[0]

TEST_WAIT_TIME_SEC = 180


def validate_credentials():
    if not os.path.exists(".env.dev"):
        print(
            f"You need to have an .env.dev file in the root directory:\n{os.getcwd()}\nYou should save your Kite username in KUSER, your Kite password in KPWD and your Kite TOTP hash in KTOTP.\nYou can save the access_token in KTOKEN after authenticating here, but leave it blank for now.\nSee help for enabling TOTP: https://tinyurl.com/pkbrokers-totp \n.env.dev file should be in the following format with values:\nKTOKEN=\nKUSER=\nKPWD=\nKTOTP=\n"
        )
        print("\nPress any key to exit...")
        return False
    return True


def kite_ticks(stop_queue=None, parent=None, test_mode=False):
    import signal

    from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher

    watcher = KiteTokenWatcher()
    print("We're now ready to begin listening to ticks from Zerodha's Kite...")
    # Store reference
    if parent is not None and hasattr(parent, "child_process_ref"):
        parent.child_process_ref = os.getpid()

    # Start stop listener
    if stop_queue is not None:
        watcher.set_stop_queue(stop_queue)

    # Set up signal handler
    def signal_handler(signum, frame):
        print(f"Received signal {signum}, stopping watcher...")
        if watcher:
            watcher.stop()
        sys.exit(0)
    try:
        if __name__ == "__main__":
            signal.signal(signal.SIGTERM, signal_handler)
    except Exception as e:
        print(e)
        pass

    if test_mode:
        import threading

        def kill_watcher():
            import time

            time.sleep(TEST_WAIT_TIME_SEC)
            watcher.stop()

        kill_thread = threading.Thread(
            target=kill_watcher, daemon=True, name="kill_watcher"
        )
        kill_thread.start()

    try:
        watcher.watch(test_mode=test_mode)
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
    from PKDevTools.classes.Environment import PKEnvironment

    from pkbrokers.kite.instrumentHistory import KiteTickerHistory
    from pkbrokers.kite.instruments import KiteInstruments

    instruments = KiteInstruments(
        api_key="kitefront", access_token=PKEnvironment().KTOKEN
    )
    tokens = instruments.get_or_fetch_instrument_tokens(all_columns=True)
    # Create history client with the full response object
    history = KiteTickerHistory(enctoken=PKEnvironment().KTOKEN)

    history.get_multiple_instruments_history(
        instruments=tokens, interval=args.history, forceFetch=True, insertOnly=True
    )
    if len(history.failed_tokens) > 0:
        history.get_multiple_instruments_history(
            instruments=history.failed_tokens,
            interval=args.history,
            forceFetch=True,
            insertOnly=True,
            past_offset=args.pastoffset if args.pastoffset else 0,
        )


def kite_instruments():
    from PKDevTools.classes.Environment import PKEnvironment

    from pkbrokers.kite.instruments import KiteInstruments

    instruments = KiteInstruments(
        api_key="kitefront", access_token=PKEnvironment().KTOKEN, recreate_schema=False
    )
    instruments.sync_instruments(force_fetch=True)
    instruments.get_or_fetch_instrument_tokens(all_columns=True)

def kite_fetch_save_pickle():
    from pkbrokers.kite.examples.externals import kite_fetch_save_pickle
    kite_fetch_save_pickle()
    
def setupLogger(logLevel=LOG_LEVEL):
    os.environ["PKDevTools_Default_Log_Level"] = str(logLevel)
    log.setup_custom_logger(
        "pkbrokers",
        logLevel,
        trace=False,
        log_file_path="PKBrokers-log.txt",
        filter=None,
    )


def try_refresh_token():
    from pkbrokers.bot.orchestrator import orchestrate_consumer

    access_token = orchestrate_consumer(command="/refresh_token")
    _save_update_environment(access_token=access_token)


def _save_update_environment(access_token: str = None):
    try:
        import os

        from PKDevTools.classes.Environment import PKEnvironment
        from PKDevTools.classes.log import default_logger

        from pkbrokers.envupdater import env_update_context

        os.environ["KTOKEN"] = access_token if access_token else PKEnvironment().KTOKEN
        default_logger().debug(f"Token received: {access_token}")
        with env_update_context(os.path.join(os.getcwd(), ".env.dev")) as updater:
            updater.update_values({"KTOKEN": access_token})
            updater.reload_env()
            default_logger().debug(
                f"Token updated in os.environment: {PKEnvironment().KTOKEN}"
            )
    except Exception as e:
        default_logger().error(f"Error while fetching remote auth token from bot: {e}")


def commit_ticks(file_name="ticks.json", branch_name="main"):
    import os

    from PKDevTools.classes import Archiver
    from PKDevTools.classes.Committer import Committer
    from PKDevTools.classes.PKDateUtilities import PKDateUtilities

    try:
        tick_file = os.path.join(Archiver.get_user_data_dir(), file_name)
        default_logger().info(f"File being committed:{tick_file}")
        if os.path.exists(tick_file):
            Committer.execOSCommand(f"git add {tick_file} -f >/dev/null 2>&1")
            commit_path = f"-A '{tick_file}'"
            default_logger().info(
                f"File being committed:{os.path.basename(tick_file)} in branch:{branch_name}"
            )
            Committer.commitTempOutcomes(
                addPath=commit_path,
                commitMessage=f"[{os.path.basename(tick_file)}-Commit-{PKDateUtilities.currentDateTime()}]",
                branchName=branch_name,
                showStatus=True,
                timeout=900,
            )
            default_logger().info(f"File committed:{tick_file}")
    except Exception as e:
        default_logger().error(f"Error commiting {tick_file} to {branch_name}: {e}")


def remote_bot_auth_token():
    from PKDevTools.classes.log import default_logger

    from pkbrokers.bot.orchestrator import orchestrate_consumer

    try:
        access_token = orchestrate_consumer(command="/token")
        _save_update_environment(access_token=access_token)
    except Exception as e:
        default_logger().error(f"Error while fetching remote auth token from bot: {e}")


def pkkite():
    if sys.platform.startswith("darwin"):
        try:
            multiprocessing.set_start_method(
                "spawn" if sys.platform.startswith("darwin") else "spawn", force=True
            )
        except RuntimeError:  # pragma: no cover
            pass

    if not validate_credentials():
        sys.exit()

    if args.auth:
        setupLogger()
        kite_auth()

    if args.ticks:
        setupLogger()
        remote_bot_auth_token()
        kite_ticks(test_mode=True if args.test else False)

    if args.history:
        import os

        from pkbrokers.kite.instrumentHistory import Historical_Interval

        supported_intervals = [member.value for member in Historical_Interval]
        if args.history not in supported_intervals:
            intervals = ", ".join(map(lambda x: x.value, Historical_Interval))
            example_lines = "\n".join(
                map(lambda x: f"--history={x.value}", Historical_Interval)
            )
            print(
                f"--history= requires at least one of the following parameters: {intervals}\nFor example:\n{example_lines}"
            )
        else:
            setupLogger()
            github_output = os.environ.get("GITHUB_OUTPUT")
            if github_output:
                print(
                    "GITHUB_OUTPUT env variable FOUND! Will use the bot to get the token."
                )
            else:
                print("Running locally? GITHUB_OUTPUT env variable NOT FOUND!")
                remote_bot_auth_token()

            try_refresh_token()
            kite_history()

    if args.instruments:
        setupLogger()
        remote_bot_auth_token()
        kite_instruments()

    if args.pickle:
        setupLogger()
        remote_bot_auth_token()
        success = kite_fetch_save_pickle()
        if success:
            import os

            from PKDevTools.classes import Archiver
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities

            exists, pickle_path = Archiver.afterMarketStockDataExists(date_suffix=True)
            if exists:
                commit_ticks(pickle_path, branch_name="actions-data-download")
            else:
                default_logger().error(
                    f"Error pickling. File does not exist: {pickle_path}."
                )

    if args.orchestrate:
        from pkbrokers.bot.orchestrator import orchestrate, orchestrate_consumer
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities
        setupLogger()
        try:
            # Let's try and get the latest ticks file from an existing running bot.
            orchestrate_consumer(command="/status")
            orchestrate_consumer(command="/ticks")
            commit_ticks(file_name="ticks.json")

            cur_ist = PKDateUtilities.currentDateTime()
            is_non_market_hour = (
                (cur_ist.hour >= 15 and cur_ist.minute >= 30)
                or (cur_ist.hour <= 9 and cur_ist.minute <= 15)
                or PKDateUtilities.isTodayHoliday()
            )
            if is_non_market_hour:
                orchestrate_consumer(command="/db")
                commit_ticks(file_name="ticks.db.zip")
        except Exception as e:
            default_logger().error(e)
            pass
        remote_bot_auth_token()
        orchestrate()

    if args.consumer:
        from pkbrokers.bot.orchestrator import orchestrate_consumer

        orchestrate_consumer(command="/ticks")

    if args.refresh_token:
        setupLogger()
        kite_auth()
        args.token = True

    if args.token:
        import os

        from pkbrokers.bot.orchestrator import orchestrate_consumer

        token = orchestrate_consumer(command="/token")
        # For GitHub Actions, write to GITHUB_OUTPUT file if the environment variable exists
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            # Append to the GITHUB_OUTPUT file with proper format
            with open(github_output, "a") as f:
                f.write(f"kite_token={token}\n")
            print("Token successfully captured for GitHub Actions")
        else:
            # Fallback for local execution
            print(f"Kite token: {token}")

    print(
        "You can use like this :\npkkite --auth\npkkite --ticks\npkkite --history\npkkite --instruments\npkkite --pickle\npkkite --orchestrate\npkkite --consumer"
    )


if __name__ == "__main__":
    log_files = ["PKBrokers-log.txt", "PKBrokers-DBlog.txt"]
    for file in log_files:
        try:
            os.remove(file)
        except BaseException:
            pass
    pkkite()
