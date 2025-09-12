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

import multiprocessing
import os
import signal
import sys
import time
from datetime import datetime
from datetime import time as dt_time
from typing import Optional

import requests

# macOS fork safety
if sys.platform.startswith("darwin"):
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
    os.environ["NO_FORK_SAFETY"] = "YES"

if __name__ == "__main__":
    multiprocessing.freeze_support()

WAIT_TIME_SEC_CLOSING_ANOTHER_RUNNING_INSTANCE = 10


class PKTickOrchestrator:
    """Orchestrates PKTickBot and kite_ticks in separate processes"""

    def __init__(
        self,
        bot_token: Optional[str] = None,
        bridge_bot_token: Optional[str] = None,
        ticks_file_path: Optional[str] = None,
        chat_id: Optional[str] = None,
    ):
        # Set spawn context globally
        multiprocessing.set_start_method("spawn", force=True)

        # Store only primitive data types that can be pickled
        self.bot_token = bot_token
        self.bridge_bot_token = bridge_bot_token
        self.ticks_file_path = ticks_file_path
        self.chat_id = chat_id
        self.bot_process = None
        self.kite_process = None
        self.mp_context = multiprocessing.get_context("spawn")
        self.manager = None
        self.child_process_ref = None
        self.stop_queue = self.mp_context.Queue()
        self.shutdown_requested = False
        self.token_generated_at_least_once = False
        self.test_mode = False

        # Don't initialize logger or other complex objects here
        # They will be initialized in each process separately

    def __getstate__(self):
        """Control what gets pickled - only include primitive data"""
        state = self.__dict__.copy()
        # Remove unpickleable objects
        for key in ["bot_process", "kite_process", "mp_context", "logger"]:
            state.pop(key, None)
        return state

    def __setstate__(self, state):
        """Restore state after unpickling"""
        self.__dict__.update(state)
        # Reinitialize multiprocessing context
        self.mp_context = multiprocessing.get_context("spawn")
        self.bot_process = None
        self.kite_process = None
        self.shutdown_requested = False

    def _get_logger(self):
        """Get logger instance - initialized separately in each process"""
        from PKDevTools.classes import log

        from pkbrokers.kite.examples.pkkite import setupLogger

        setupLogger()
        return log.default_logger()

    def _initialize_environment(self):
        """Initialize environment variables if not provided"""
        if not self.bot_token or not self.chat_id or not self.ticks_file_path:
            from PKDevTools.classes import Archiver
            from PKDevTools.classes.Environment import PKEnvironment

            env = PKEnvironment()
            self.bot_token = self.bot_token or env.TBTOKEN
            self.bridge_bot_token = self.bridge_bot_token or env.BBTOKEN
            self.chat_id = self.chat_id or env.CHAT_ID
            self.ticks_file_path = self.ticks_file_path or os.path.join(
                Archiver.get_user_data_dir(), "ticks.json"
            )

    def is_market_hours(self):
        """Check if current time is within NSE market hours (9:15 AM to 3:30 PM IST)"""
        try:
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities

            # Get current time in IST (UTC+5:30)
            utc_now = datetime.utcnow()
            ist_now = PKDateUtilities.utc_to_ist(
                utc_dt=utc_now
            )  # utc_now.replace(hour=utc_now.hour + 5, minute=utc_now.minute + 30)

            # Market hours: 9:15 AM to 3:30 PM IST
            market_start = dt_time(9, 0)
            market_end = dt_time(17, 30)

            # Check if within market hours
            current_time = ist_now.time()
            return market_start <= current_time <= market_end

        except Exception as e:
            print(f"Error checking market hours: {e}")
            return False

    def is_trading_holiday(self):
        """Check if today is a trading holiday"""
        try:
            # Download holidays JSON
            response = requests.get(
                "https://raw.githubusercontent.com/pkjmesra/PKScreener/main/.github/dependencies/nse-holidays.json",
                timeout=10,
            )
            response.raise_for_status()
            holidays_data = response.json()

            # Get current date in DD-MMM-YYYY format (e.g., 26-Jan-2025)
            current_date = datetime.now().strftime("%d-%b-%Y")

            # Check if current date is in holidays list under "CM" key
            trading_holidays = holidays_data.get("CM", [])
            for holiday in trading_holidays:
                if holiday.get("tradingDate") == current_date:
                    return True

            return False

        except Exception as e:
            print(f"Error checking trading holidays: {e}")
            return False  # Assume not holiday if we can't check

    def should_run_kite_process(self):
        """Determine if kite process should run based on market hours and holidays"""
        # Check if it's a trading holiday
        if self.is_trading_holiday():
            return False

        # Check if it's market hours
        if not self.is_market_hours():
            return False

        return True

    def run_kite_ticks(self):
        """Run kite_ticks in a separate process"""
        try:
            # Initialize environment and logger in this process
            self._initialize_environment()
            logger = self._get_logger()

            from pkbrokers.kite.examples.pkkite import kite_ticks

            logger.info("Starting kite_ticks process...")
            self.manager = multiprocessing.Manager()
            kite_ticks(stop_queue=self.stop_queue, parent=self)
        except KeyboardInterrupt:
            logger.info("kite_ticks process interrupted")
        except Exception as e:
            logger.error(f"kite_ticks error: {e}")

    def run_telegram_bot(self):
        """Run Telegram bot in a separate process"""
        try:
            # Initialize environment and logger in this process
            self._initialize_environment()
            logger = self._get_logger()

            from pkbrokers.bot.tickbot import PKTickBot

            logger.info("Starting PKTickBot process...")

            # Create and run the bot
            bot = PKTickBot(self.bot_token, self.ticks_file_path, self.chat_id)
            bot.run(parent=self)

        except Exception as e:
            logger.error(f"Telegram bot error: {e}")

    def bot_callback(self):
        self.test_mode = True

    def start(self):
        """Start both processes based on market conditions"""
        # Initialize logger in main process
        logger = self._get_logger()
        logger.info("Starting PKTick Orchestrator...")

        # Always start Telegram bot process
        self.bot_process = self.mp_context.Process(
            target=self.run_telegram_bot, name="PKTickBotProcess"
        )
        self.bot_process.daemon = False
        self.bot_process.start()
        logger.info("Telegram bot process started")
        time.sleep(WAIT_TIME_SEC_CLOSING_ANOTHER_RUNNING_INSTANCE)
        from pkbrokers.bot.tickbot import conflict_detected

        while True:
            if conflict_detected:
                conflict_detected = False
                time.sleep(WAIT_TIME_SEC_CLOSING_ANOTHER_RUNNING_INSTANCE)
            else:
                break

        # Start kite_ticks process only during market hours and non-holidays
        if self.should_run_kite_process():
            time.sleep(WAIT_TIME_SEC_CLOSING_ANOTHER_RUNNING_INSTANCE)
            self.kite_process = self.mp_context.Process(
                target=self.run_kite_ticks, name="KiteTicksProcess"
            )
            self.kite_process.daemon = False
            self.kite_process.start()
            logger.info("Kite ticks process started (market hours)")
        else:
            logger.info(
                "Kite ticks process not started (outside market hours or holiday)"
            )
            kite_running = self.kite_process and self.kite_process.is_alive()
            if kite_running:
                processes = [(self.kite_process, "kite process")]
                self.stop(processes=processes)
            self.kite_process = None
            from pkbrokers.kite.examples.pkkite import commit_ticks

            commit_ticks(file_name="ticks.json")
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities

            cur_ist = PKDateUtilities.currentDateTime()
            is_non_market_hour = (
                (cur_ist.hour >= 15 and cur_ist.minute >= 30)
                and (cur_ist.hour <= 9 and cur_ist.minute <= 15)
                or PKDateUtilities.isTodayHoliday()
            )
            if is_non_market_hour:
                commit_ticks(file_name="ticks.db.zip")

    def stop(self, processes=[]):
        """Stop both processes gracefully with proper resource cleanup"""
        logger = self._get_logger()
        logger.info("Stopping processes...")

        # Try to stop watcher through queue
        try:
            self.stop_queue.put("STOP")
            time.sleep(2)  # Give time to process
        except Exception as e:
            logger.error(f"Error sending stop signal to KiteTokenWatcher: {e}")

        # Force stop if still running
        if self.child_process_ref is not None:
            logger.info("Child processes is being requested to be stopped")
            watcher_pid = self.child_process_ref
            if watcher_pid:
                try:
                    os.kill(watcher_pid, signal.SIGTERM)
                    time.sleep(2)
                    logger.info("Sent stop signal to watcher process")
                except ProcessLookupError:
                    logger.info("Watcher process already terminated")
                except Exception as e:
                    logger.error(f"Error signaling watcher: {e}")
            else:
                logger.warn("No child processes exists in the dict!")
        else:
            logger.warn("No child processes was found!")

        # Stop processes with proper cleanup
        processes = (
            [(self.kite_process, "kite process"), (self.bot_process, "bot process")]
            if len(processes) == 0
            else processes
        )

        for process, name in processes:
            if process and process.is_alive():
                try:
                    logger.info(f"Stopping {name}...")
                    process.terminate()
                    process.join(timeout=3)

                    if process.is_alive():
                        logger.warning(
                            f"{name} did not terminate gracefully, forcing..."
                        )
                        process.kill()
                        process.join(timeout=2)

                    # Close to release resources
                    process.close()

                except Exception as e:
                    logger.error(f"Error stopping {name}: {e}")
                finally:
                    if name == "kite process":
                        self.kite_process = None
                    else:
                        self.bot_process = None

        # Force resource cleanup
        self._cleanup_multiprocessing_resources()
        logger.info("All processes stopped and resources cleaned up")

    def _cleanup_multiprocessing_resources(self):
        """Clean up multiprocessing resources"""
        try:
            import gc

            gc.collect()

            # Clean up any remaining semaphores
            import multiprocessing.synchronize

            for obj in gc.get_objects():
                if isinstance(obj, multiprocessing.synchronize.Semaphore):
                    try:
                        obj._semaphore.close()
                    except BaseException:
                        pass
        except Exception as e:
            self._get_logger().debug(f"Resource cleanup note: {e}")

    def restart_kite_process_if_needed(self):
        """Restart kite process if market conditions change"""
        logger = self._get_logger()
        if self.test_mode:
            logger.warn("Running in TEST mode! Skipping test to re-run/stop Kite process!")
            return
        current_should_run = self.should_run_kite_process()
        kite_running = self.kite_process and self.kite_process.is_alive()

        # If kite should run but isn't running, start it
        if current_should_run and not kite_running:
            logger.info("Market hours started - starting kite process")
            self.kite_process = self.mp_context.Process(
                target=self.run_kite_ticks, name="KiteTicksProcess"
            )
            self.kite_process.daemon = False
            self.kite_process.start()

        # If kite is running but shouldn't be, stop it
        elif not current_should_run and kite_running:
            logger.info("Market hours ended - stopping kite process")
            self.kite_process.terminate()
            self.kite_process.join(timeout=5)
            self.kite_process = None

    def run(self):
        """Main run method with graceful shutdown handling"""
        try:
            logger = self._get_logger()
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
            self.start()

            # Keep main process alive and monitor child processes
            logger = self._get_logger()
            last_market_check = time.time()
            from PKDevTools.classes.GitHubSecrets import PKGitHubSecretsManager

            gh_manager = PKGitHubSecretsManager(repo="pkbrokers")
            gh_manager.test_encryption()
            test_mode_counter = 0
            while True:
                time.sleep(1)

                # Check if shutdown was requested (e.g., due to conflict)
                if self.shutdown_requested:
                    logger.info(
                        "Shutdown requested due to conflict. Stopping processes..."
                    )
                    break

                # Check if bot process died
                if self.bot_process and not self.bot_process.is_alive():
                    # Check if bot died due to conflict
                    if self._check_bot_exit_status():
                        logger.warn(
                            "Bot process died due to conflict. Shutting down orchestrator..."
                        )
                        break
                    else:
                        logger.warn("Bot process died, restarting...")
                        self.bot_process = self.mp_context.Process(
                            target=self.run_telegram_bot, name="PKTickBotProcess"
                        )
                        self.bot_process.daemon = False
                        self.bot_process.start()

                # Check market conditions every 30 seconds for kite process
                current_time = time.time()
                if (
                    current_time - last_market_check > 30
                    and not self.shutdown_requested
                ):
                    self.restart_kite_process_if_needed()
                    if self.test_mode:
                        test_mode_counter += 1
                    if test_mode_counter >= 5:
                        self.test_mode = False
                        test_mode_counter = 0
                    last_market_check = current_time
                    # If it's around 7:30AM IST, let's re-generate the kite token once a day each morning
                    # https://kite.trade/forum/discussion/7759/access-token-validity
                    from PKDevTools.classes.Environment import PKEnvironment
                    from PKDevTools.classes.PKDateUtilities import PKDateUtilities

                    cur_ist = PKDateUtilities.currentDateTime()
                    is_token_generation_hour = (
                        cur_ist.hour >= 7 and cur_ist.minute >= 30
                    ) and (cur_ist.hour <= 8 and cur_ist.minute <= 30)
                    if (
                        not self.token_generated_at_least_once
                        and is_token_generation_hour
                    ):
                        from PKDevTools.classes.GitHubSecrets import (
                            PKGitHubSecretsManager,
                        )

                        logger.info(
                            f"CI_PAT length:{len(PKEnvironment().CI_PAT)}. Value: {PKEnvironment().CI_PAT[:10]}"
                        )
                        secrets_manager = PKGitHubSecretsManager(
                            repo="pkbrokers", token=PKEnvironment().CI_PAT
                        )
                        try:
                            secret_info = secrets_manager.get_secret("KTOKEN")
                            if secret_info:
                                last_updated_utc = secret_info["updated_at"]
                                last_updated_ist = PKDateUtilities.utc_str_to_ist(
                                    last_updated_utc
                                )
                                if last_updated_ist.date() != cur_ist.date():
                                    from pkbrokers.kite.examples.pkkite import kite_auth

                                    kite_auth()
                                    self.token_generated_at_least_once = True
                        except Exception as e:
                            logger.error(f"Error while updating token: {e}")

            self.stop()

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Unexpected error in orchestrator: {e}")
        finally:
            self.stop()
            logger.info("Orchestrator stopped completely")

    def _check_bot_exit_status(self):
        """Check if bot process exited due to conflict"""
        from pkbrokers.bot.tickbot import conflict_detected

        if conflict_detected:
            self.shutdown_requested = True
            return True
        if self.bot_process and self.bot_process.exitcode is not None:
            # If bot exited with non-zero code, it might be due to conflict
            if self.bot_process.exitcode != 0:
                self.shutdown_requested = True
                return True
        return False

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger = self._get_logger()
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.shutdown_requested = True

    def get_consumer(self):
        """Get a consumer instance to interact with the bot"""
        self._initialize_environment()
        from pkbrokers.bot.consumer import PKTickBotConsumer

        if not self.chat_id:
            raise ValueError("chat_id is required for consumer functionality")
        return PKTickBotConsumer(self.bot_token, self.bridge_bot_token, self.chat_id)


def orchestrate():
    # Initialize with None values, they will be set from environment when needed
    orchestrator = PKTickOrchestrator(None, None, None, None)
    orchestrator.run()


def orchestrate_consumer(command: str = "/ticks"):
    import json

    from PKDevTools.classes import Archiver

    from pkbrokers.bot.consumer import try_get_command_response_from_bot

    # Programmatic usage with zip handling
    # orchestrator = PKTickOrchestrator(None, None, None, None)
    # consumer = orchestrator.get_consumer()
    # success, json_path = consumer.get_ticks(output_dir=os.path.join(Archiver.get_user_data_dir()))
    response = try_get_command_response_from_bot(command=command)
    success = response["success"]
    if response["type"] in ["file"]:
        file_name = "ticks.json" if command == "/ticks" else "ticks.db"
        file_path = os.path.join(Archiver.get_user_data_dir(), file_name)
        if success and os.path.exists(file_path):
            print(f"✅ Downloaded and extracted {file_name} to: {file_path}")
            if file_name.endswith(".json"):
                # Now you can use the JSON file
                with open(file_path, "r") as f:
                    data = json.load(f)
                print(f"Found {len(data)} instruments")
        else:
            print("❌ Failed to get ticks file")
    elif response["type"] in ["photo"]:
        print("We can also get photo")
    elif response["type"] in ["text"]:
        if command in ["/token", "refresh_token"]:
            from PKDevTools.classes.Environment import PKEnvironment

            from pkbrokers.envupdater import env_update_context

            prev_token = PKEnvironment().KTOKEN
            with env_update_context(os.path.join(os.getcwd(), ".env.dev")) as updater:
                updater.update_values({"KTOKEN": response["content"]})
                updater.reload_env()
                new_token = PKEnvironment().KTOKEN
            print(f"Token updated:{prev_token != new_token}")
        return response["content"]
