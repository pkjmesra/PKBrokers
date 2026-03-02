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


def kite_ticks(stop_queue=None, parent=None, test_mode=False, ws_stop_event=None, shared_stats=None):
    import signal

    from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher

    print(f"kite_ticks received ws_stop_event: {ws_stop_event}")
    watcher = KiteTokenWatcher(shared_stats=shared_stats)
    print("We're now ready to begin listening to ticks from Zerodha's Kite...")
    
    # Store the stop event in watcher for WebSocket processes
    if ws_stop_event:
        print(f"Setting ws_stop_event in watcher: {ws_stop_event}")
        watcher.set_ws_stop_event(ws_stop_event)
    
    # Store reference
    if parent is not None: # Get the current process ID
        current_pid = os.getpid()
        print(f"Setting child_process_ref to PID: {current_pid}")
        
        # # Check if parent has the attribute and set it
        # if hasattr(parent, "child_process_ref"):
        #     parent.child_process_ref = current_pid
        # else:
        #     # If the attribute doesn't exist, create it
        #     parent.child_process_ref = current_pid
            
        # Also try to set it via the provided method if it exists
        if hasattr(parent, "set_child_pid"):
            parent.set_child_pid(current_pid)

    if stop_queue is None:
        mp_context = multiprocessing.get_context("spawn")
        stop_queue = mp_context.Queue()
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
        # if __name__ == "__main__":
        signal.signal(signal.SIGTERM, signal_handler)
    except Exception as e:
        print(e)
        pass

    if test_mode:
        import threading

        def kill_watcher():
            import time

            time.sleep(TEST_WAIT_TIME_SEC)
            if stop_queue is not None:
                stop_queue.put("STOP")
            if watcher:
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
        from PKDevTools.classes.log import default_logger
        default_logger().error(f"Kite ticks watcher error: {e}")
        watcher.stop()


def kite_auth():
    # Configuration - load from environment in production
    from pkbrokers.kite.examples.externals import kite_auth
    kite_auth()


def kite_history():
    from PKDevTools.classes.Environment import PKEnvironment

    from pkbrokers.kite.instrumentHistory import KiteTickerHistory
    from pkbrokers.kite.instruments import KiteInstruments

    instruments = KiteInstruments(
        api_key="kitefront", access_token=PKEnvironment().KTOKEN, 
        local=PKEnvironment().DB_TYPE == "local"
    )
    tokens = instruments.get_or_fetch_instrument_tokens(all_columns=True)
    # Create history client with the full response object
    history = KiteTickerHistory(enctoken=PKEnvironment().KTOKEN)

    if args.pastoffset:
        try:
            past_offset = int(args.pastoffset)
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            from datetime import timedelta
            from_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime() - timedelta(days=365)
            ) if past_offset == 0 else PKDateUtilities.nthPastTradingDateStringFromFutureDate(n=past_offset)
            print(f"from_date set to {from_date} for --pastoffset: {args.pastoffset}. This means the history data will be fetched starting from {from_date}.")
        except ValueError:
            print(f"Invalid value for --pastoffset: {args.pastoffset}. It should be an integer representing the number of past trading days to fetch data for.")
            sys.exit(1)
    history.get_multiple_instruments_history(
        instruments=tokens, interval=args.history, forceFetch=True, insertOnly=True, past_offset=int(args.pastoffset) if args.pastoffset else 0,
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
    return kite_fetch_save_pickle()
    
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


def save_optimized_format(ticks_data, output_dir=None):
    """
    Phase 6 Optimization: Save tick data in compressed binary format for faster loading.
    Uses gzip compression on JSON for ~70% size reduction and faster network transfer.
    
    Args:
        ticks_data: Dictionary of tick data
        output_dir: Output directory (defaults to user data dir)
    
    Returns:
        Path to the saved file
    """
    import gzip
    import json
    import os

    from PKDevTools.classes import Archiver

    if output_dir is None:
        output_dir = Archiver.get_user_data_dir()

    try:
        # Save as gzipped JSON (good balance of speed and compression)
        output_path = os.path.join(output_dir, "ticks_optimized.json.gz")
        with gzip.open(output_path, "wt", encoding="utf-8") as f:
            json.dump(ticks_data, f, separators=(",", ":"))  # Compact JSON
        
        default_logger().info(f"Saved optimized format: {output_path}")
        return output_path
    except Exception as e:
        default_logger().error(f"Error saving optimized format: {e}")
        return None


def remote_bot_auth_token():
    from PKDevTools.classes.log import default_logger

    from pkbrokers.bot.orchestrator import orchestrate_consumer

    try:
        access_token = orchestrate_consumer(command="/token")
        # If token is None or empty, try refresh_token to generate a new one
        if not access_token or access_token == "None" or len(str(access_token).strip()) < 10:
            default_logger().info("Token is None or invalid, requesting /refresh_token...")
            access_token = orchestrate_consumer(command="/refresh_token")
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
        from pkbrokers.bot.dataSharingManager import get_data_sharing_manager
        setupLogger()
        
        data_mgr = get_data_sharing_manager()
        
        try:
            # Let's try and get the latest ticks file from an existing running bot.
            orchestrate_consumer(command="/status")
            orchestrate_consumer(command="/ticks")
            commit_ticks(file_name="ticks.json")
            
            # Request pkl files from running instance
            try:
                default_logger().info("Requesting data files from running instance...")
                orchestrate_consumer(command="/request_data")
                
                # Also request individual pkl files
                orchestrate_consumer(command="/daily_pkl")
                orchestrate_consumer(command="/intraday_pkl")
                
                data_mgr.data_received_from_instance = True
                default_logger().info("Received data from running instance")
            except Exception as pkl_error:
                default_logger().warning(f"Could not get pkl files from running instance: {pkl_error}")
                
                # Try GitHub fallback
                default_logger().info("Trying GitHub fallback for pkl files...")
                success_daily, _ = data_mgr.download_from_github(file_type="daily")
                success_intraday, _ = data_mgr.download_from_github(file_type="intraday")
                
                if success_daily or success_intraday:
                    default_logger().info("Downloaded data from GitHub actions-data-download branch")
                else:
                    default_logger().info("No fallback data available, starting fresh")

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
    exit(0)


if __name__ == "__main__":
    log_files = ["PKBrokers-log.txt", "PKBrokers-DBlog.txt"]
    for file in log_files:
        try:
            os.remove(file)
        except BaseException:
            pass
    pkkite()
    exit(0)
