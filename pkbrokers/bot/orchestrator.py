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

from asyncio.log import logger
import multiprocessing
import os
import signal
import sys
import time
from datetime import datetime
from datetime import time as dt_time
from typing import Optional

import requests
from PKDevTools.classes.log import default_logger

# macOS fork safety
if sys.platform.startswith("darwin"):
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
    os.environ["NO_FORK_SAFETY"] = "YES"

if __name__ == "__main__":
    multiprocessing.freeze_support()

WAIT_TIME_SEC_CLOSING_ANOTHER_RUNNING_INSTANCE = 10

class StatsCollector:
    """Dedicated process to collect and serve stats"""
    
    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.stats = self.manager.dict({
            'instrument_count': 0,
            'instruments_with_ticks': 0,
            'ticks_processed': 0,
            'uptime_seconds': 0,
            'candles_created': 0,
            'candles_completed': 0,
            'last_update': time.time()
        })
        self.update_queue = self.manager.Queue()
        self.stop_event = self.manager.Event()
        self.process = None
    
    def start(self):
        """Start the stats collector process"""
        self.process = multiprocessing.Process(target=self._run)
        self.process.daemon = True
        self.process.start()
    
    def _run(self):
        """Main collector loop"""
        logger = default_logger()
        logger.info("Stats collector started")
        
        while not self.stop_event.is_set():
            try:
                # Process any updates from the queue
                while not self.update_queue.empty():
                    update = self.update_queue.get_nowait()
                    if isinstance(update, dict):
                        for key, value in update.items():
                            if key in self.stats:
                                self.stats[key] = value
                
                # Update uptime
                self.stats['uptime_seconds'] = time.time() - self.stats.get('start_time', time.time())
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Stats collector error: {e}")
    
    def get_stats(self):
        """Get current stats (safe to call from any process)"""
        return dict(self.stats)
    
    def update(self, updates: dict):
        """Send updates to the collector"""
        try:
            self.update_queue.put(updates)
        except Exception:
            pass
    
    def stop(self):
        """Stop the collector"""
        self.stop_event.set()
        if self.process:
            self.process.join(timeout=5)

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
        # Create stats collector
        # self.stats_collector = StatsCollector()
        self.mp_context = multiprocessing.get_context("spawn")
        self.manager = multiprocessing.Manager()
        self.shared_stats = self.manager.dict()
        # Initialize with some values to verify
        self.shared_stats['orchestrator_created'] = True
        self.shared_stats['orchestrator_pid'] = os.getpid()
        self.shared_stats['instrument_count'] = 0
        self.shared_stats['instruments_with_ticks'] = 0
        self.shared_stats['ticks_processed'] = 0
        self.shared_stats['uptime_seconds'] = 0
        self.shared_stats['candles_created'] = 0
        self.shared_stats['candles_completed'] = 0
        self.shared_stats['last_tick_time'] = 0
        self.shared_stats['start_time'] = time.time()
        # # For backward compatibility, provide a dict-like interface
        # self.shared_stats = self.stats_collector.stats
        
        logger = default_logger()
        logger.debug(f"Orchestrator shared_stats created: {dict(self.shared_stats)}")
        self.child_process_ref = self.mp_context.Value("i", 0)
        self.stop_queue = self.manager.Queue()
        self.ws_stop_event = self.manager.Event()

        self.shutdown_requested = False
        self.token_generated_at_least_once = False
        self.test_mode = False
        self.ws_processes = []
        self.stats_queue = self.manager.Queue()

        # Don't initialize logger or other complex objects here
        # They will be initialized in each process separately

    def __getstate__(self):
        """Control what gets pickled - explicitly define only known pickleable attributes"""
        state = {}
        # Primitive attributes
        state['bot_token'] = self.bot_token
        state['bridge_bot_token'] = self.bridge_bot_token
        state['ticks_file_path'] = self.ticks_file_path
        state['chat_id'] = self.chat_id
        state['shutdown_requested'] = self.shutdown_requested
        state['token_generated_at_least_once'] = self.token_generated_at_least_once
        state['test_mode'] = self.test_mode
        state['ws_processes'] = self.ws_processes # Should be an empty list

        # Multiprocessing objects managed by Manager - these are pickleable by reference
        state['manager'] = self.manager
        state['shared_stats'] = self.shared_stats
        state['child_process_ref'] = self.child_process_ref
        state['stop_queue'] = self.stop_queue
        state['ws_stop_event'] = self.ws_stop_event
        
        return state

    def __setstate__(self, state):
        """Restore state after unpickling"""
        self.bot_token = state['bot_token']
        self.bridge_bot_token = state['bridge_bot_token']
        self.ticks_file_path = state['ticks_file_path']
        self.chat_id = state['chat_id']
        self.shutdown_requested = state['shutdown_requested']
        self.token_generated_at_least_once = state['token_generated_at_least_once']
        self.test_mode = state['test_mode']
        self.ws_processes = state['ws_processes']

        self.manager = state['manager']
        self.shared_stats = state['shared_stats']
        self.child_process_ref = state['child_process_ref']
        self.stop_queue = state['stop_queue']
        self.ws_stop_event = state['ws_stop_event']

        # Re-initialize multiprocessing context and processes as they are not pickled
        self.mp_context = multiprocessing.get_context("spawn")
        self.bot_process = None
        self.kite_process = None





    def set_child_pid(self, pid):
        """Method to set the child process PID from the child process"""
        self.child_process_ref.value = pid
        from PKDevTools.classes.log import default_logger
        logger = default_logger()
        logger.debug(f"Child process PID set to: {pid}")

    def is_market_hours(self):
        """Check if current time is within NSE market hours (9:15 AM to 3:30 PM IST)"""
        try:
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            from datetime import timezone

            # Get current time in IST (UTC+5:30)
            utc_now = datetime.now(timezone.utc)
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
            from PKDevTools.classes.log import default_logger
            logger = default_logger()
            logger.debug(f"Error checking market hours: {e}")
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
            from PKDevTools.classes.log import default_logger
            logger = default_logger()
            logger.debug(f"Error checking trading holidays: {e}")
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

    @staticmethod
    def run_kite_ticks(bot_token: Optional[str], ticks_file_path: Optional[str], chat_id: Optional[str], shared_stats: dict, stats_queue, child_process_ref, ws_stop_event, stop_queue):
        """Run kite_ticks in a separate process"""
        # Initialize logger in this process
        from PKDevTools.classes import log
        import threading
        from pkbrokers.kite.examples.pkkite import setupLogger
        setupLogger()
        logger = log.default_logger()
        # Debug - log the shared_stats at entry
        logger.debug(f"run_kite_ticks received shared_stats: {dict(shared_stats) if shared_stats else 'None'}")
        logger.debug(f"shared_stats type: {type(shared_stats)}")

        # Initialize environment variables
        from PKDevTools.classes import Archiver
        from PKDevTools.classes.Environment import PKEnvironment

        env = PKEnvironment()
        bot_token = bot_token or env.TBTOKEN # this is not used here but for consistency
        chat_id = chat_id or env.CHAT_ID # this is not used here but for consistency
        ticks_file_path = ticks_file_path or os.path.join(
            Archiver.get_user_data_dir(), "ticks.json"
        )
        
        try:
            # Ensure we have a valid token before starting kite_ticks
            token = PKEnvironment().KTOKEN
            if not token or token == "None" or len(str(token).strip()) < 10:
                logger.info("No valid KTOKEN found, authenticating with Kite...")
                try:
                    from pkbrokers.kite.examples.externals import kite_auth
                    kite_auth()
                    logger.info("Kite authentication successful")
                except Exception as auth_e:
                    logger.error(f"Kite authentication failed: {auth_e}")
                    logger.warning("Proceeding without valid token - WebSocket will fail")
            
            from pkbrokers.kite.examples.pkkite import kite_ticks
            
            logger.debug(f"Starting kite_ticks process with ws_stop_event: {ws_stop_event}")
            # If shared_stats is None, try to recreate it
            if shared_stats is None:
                logger.warning("shared_stats is None, creating new manager dict")
                from multiprocessing import Manager
                manager = Manager()
                shared_stats = manager.dict()
                shared_stats['instrument_count'] = 0
                shared_stats['instruments_with_ticks'] = 0
                shared_stats['ticks_processed'] = 0
                shared_stats['uptime_seconds'] = 0
                shared_stats['candles_created'] = 0
                shared_stats['candles_completed'] = 0
                shared_stats['last_tick_time'] = 0
                shared_stats['start_time'] = time.time()
            # Start a thread to send stats updates to the queue
            def stats_sender():
                last_send = time.time()
                last_ticks_commit = time.time()
                logger.info("stats_sender started successfully!")
                CYCLE_TIME_SEC = 30
                commit_count = 0
                error_count = 0
                
                while True:
                    try:
                        time.sleep(CYCLE_TIME_SEC)
                        
                        # Send stats updates
                        if time.time() - last_send >= CYCLE_TIME_SEC:
                            stats_queue.put(shared_stats.copy())
                            last_send = time.time()
                        
                        # Check if it's time to commit ticks (every 60 seconds)
                        current_time = time.time()
                        time_since_last_commit = current_time - last_ticks_commit
                        
                        if time_since_last_commit >= 2*CYCLE_TIME_SEC:  # 60 seconds
                            logger.debug(f"Commit check: {time_since_last_commit:.1f}s since last commit")
                            
                            from pkbrokers.kite.examples.pkkite import commit_ticks
                            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
                            
                            cur_ist = PKDateUtilities.currentDateTime()
                            
                            # Market hours: 9:15 AM to 3:30 PM on non-holiday weekdays
                            is_market_open = (
                                cur_ist.weekday() < 5 and  # Monday=0, Friday=4
                                not PKDateUtilities.isTodayHoliday()[0] and
                                (
                                    (cur_ist.hour == 9 and cur_ist.minute >= 15) or  # 9:15 AM to 9:59 AM
                                    (cur_ist.hour > 9 and cur_ist.hour < 15) or      # 10:00 AM to 2:59 PM
                                    (cur_ist.hour == 15 and cur_ist.minute <= 30)    # 3:00 PM to 3:30 PM
                                )
                            )
                            
                            # Force commit if market is open AND if it's been more than 4 cycles (2 minutes) since last commit
                            should_commit = is_market_open and (time_since_last_commit >= 4*CYCLE_TIME_SEC)
                            
                            logger.info(f"Commit decision: should_commit={should_commit}, is_market_open={is_market_open}, time_since={time_since_last_commit:.1f}s")
                            
                            if should_commit:
                                try:
                                    commit_count += 1
                                    logger.info(f"Attempting commit #{commit_count} at {cur_ist.strftime('%H:%M:%S')}")
                                    commit_ticks(file_name="ticks.json")
                                    last_ticks_commit = current_time
                                    logger.info(f"✓ Commit #{commit_count} successful")
                                except Exception as commit_err:
                                    error_count += 1
                                    logger.error(f"✗ Commit #{commit_count} failed (error #{error_count}): {commit_err}", exc_info=True)
                                    # Don't update last_ticks_commit on failure, so it will retry
                    except Exception as e:
                        logger.error(f"Fatal error in stats_sender: {e}", exc_info=True)
                        time.sleep(5)  # Brief pause before retrying
            threading.Thread(target=stats_sender, daemon=True).start()
            kite_ticks(stop_queue=stop_queue, ws_stop_event=ws_stop_event, shared_stats=shared_stats, child_process_ref=child_process_ref)
        except KeyboardInterrupt:
            logger.info("kite_ticks process interrupted")
        except Exception as e:
            logger.error(f"kite_ticks error: {e}")

    @staticmethod
    def run_telegram_bot(bot_token: Optional[str], ticks_file_path: Optional[str], chat_id: Optional[str], shared_stats: dict):
        """Run Telegram bot in a separate process"""
        # Initialize logger in this process
        from PKDevTools.classes import log
        from pkbrokers.kite.examples.pkkite import setupLogger
        setupLogger()
        logger = log.default_logger()

        # Initialize environment variables
        from PKDevTools.classes import Archiver
        from PKDevTools.classes.Environment import PKEnvironment

        env = PKEnvironment()
        bot_token = bot_token or env.TBTOKEN
        chat_id = chat_id or env.CHAT_ID
        ticks_file_path = ticks_file_path or os.path.join(
            Archiver.get_user_data_dir(), "ticks.json"
        )
        
        try:
            from pkbrokers.bot.tickbot import PKTickBot

            logger.info("Starting PKTickBot process...")

            # Create and run the bot
            bot = PKTickBot(bot_token, ticks_file_path, chat_id, shared_stats=shared_stats)
            bot.run()

        except Exception as e:
            logger.error(f"Telegram bot error: {e}")

    def bot_callback(self):
        if hasattr(self, "test_mode"):
            self.test_mode = True

    def start(self):
        """Start both processes based on market conditions"""
        # Initialize logger in main process
        from PKDevTools.classes.log import default_logger
        logger = default_logger()
        logger.info("Starting PKTick Orchestrator...")
        # self.stats_collector.start()
        # Always start Telegram bot process
        self.bot_process = self.mp_context.Process(
            target=PKTickOrchestrator.run_telegram_bot, 
            args=(self.bot_token, self.ticks_file_path, self.chat_id, self.shared_stats,), 
            name="PKTickBotProcess"
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
            self.kite_process = self.mp_context.Process(
                target=PKTickOrchestrator.run_kite_ticks, 
                args=(self.bot_token, self.ticks_file_path, self.chat_id, 
                      self.shared_stats, self.stats_queue, self.child_process_ref, 
                      self.ws_stop_event, self.stop_queue), 
                name="KiteTicksProcess"
            )
            logger.debug(f"start: Orchestrator shared_stats id: {id(self.shared_stats)}")
            logger.debug(f"start: Orchestrator shared_stats type: {type(self.shared_stats)}")
            self.kite_process.daemon = False
            self.kite_process.start()
            logger.info("Kite ticks process started (market hours)")
        else:
            logger.warning(
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
                or  (cur_ist.hour <= 9 and cur_ist.minute <= 15)
                or PKDateUtilities.isTodayHoliday()[0]
            )
            if is_non_market_hour:
                commit_ticks(file_name="ticks.db.zip")

    def stop(self, processes=[]):
        """Stop both processes gracefully with proper resource cleanup"""
        from PKDevTools.classes.log import default_logger
        logger = default_logger()
        logger.warning("Stopping processes...")
        # self.stats_collector.stop()
        # Set WebSocket stop event if it exists
        if self.ws_stop_event:
            logger.warning("Signaling WebSocket processes to stop...")
            self.ws_stop_event.set()

        # Try to stop watcher through queue for a graceful shutdown
        try:
            self.stop_queue.put("STOP")
            time.sleep(2)  # Give time to process
        except Exception as e:
            logger.error(f"Error sending stop signal to KiteTokenWatcher: {e}")

        # Stop processes with proper cleanup
        processes = (
            [(self.kite_process, "kite process"), (self.bot_process, "bot process")]
            if len(processes) == 0
            else processes
        )

        for process, name in processes:
            if process and process.is_alive():
                try:
                    logger.warning(f"Stopping {name} (PID: {process.pid})...")
                    # Give more time for kite_process to shutdown gracefully
                    join_timeout = 10
                    kill_timeout = 5
                    
                    process.terminate()  # Sends SIGTERM
                    process.join(timeout=join_timeout)

                    if process.is_alive():
                        logger.warning(
                            f"{name} did not terminate gracefully after {join_timeout}s, forcing..."
                        )
                        process.kill() # Sends SIGKILL
                        process.join(timeout=kill_timeout)

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
            from PKDevTools.classes.log import default_logger
            logger = default_logger()
            logger.debug(f"Resource cleanup note: {e}")

    def restart_kite_process_if_needed(self):
        """Restart kite process if market conditions change"""
        from PKDevTools.classes.log import default_logger
        logger = default_logger()
        if self.test_mode:
            logger.warn("Running in TEST mode! Skipping test to re-run/stop Kite process!")
            return
        current_should_run = self.should_run_kite_process()
        kite_running = self.kite_process and self.kite_process.is_alive()

        # If kite should run but isn't running, start it
        if current_should_run and not kite_running:
            logger.info("Market hours started - starting kite process")
            self.kite_process = self.mp_context.Process(
                target=PKTickOrchestrator.run_kite_ticks, 
                args=(self.bot_token, self.ticks_file_path, self.chat_id, 
                      self.shared_stats, self.stats_queue, self.child_process_ref, 
                      self.ws_stop_event, self.stop_queue), 
                name="KiteTicksProcess"
            )
            logger.debug(f"restart_kite_process_if_needed: Orchestrator shared_stats id: {id(self.shared_stats)}")
            logger.debug(f"restart_kite_process_if_needed: Orchestrator shared_stats type: {type(self.shared_stats)}")
            self.kite_process.daemon = False
            self.kite_process.start()

        # If kite is running but shouldn't be, stop it
        elif not current_should_run and kite_running:
            logger.warning("Market hours ended - stopping kite process")
            self.kite_process.terminate()
            self.kite_process.join(timeout=5)
            self.kite_process = None

    def run(self):
        """Main run method with graceful shutdown handling"""
        try:
            from PKDevTools.classes.log import default_logger
            import threading
            import queue
            logger = default_logger()
            # Start a thread to read from stats_queue and update shared_stats
            def stats_reader():
                logger = default_logger()
                while not self.shutdown_requested:
                    try:
                        stats_update = self.stats_queue.get(timeout=1)
                        # Update the shared_stats with values from the child process
                        for key, value in stats_update.items():
                            self.shared_stats[key] = value
                        logger.debug(f"Updated shared_stats from queue: {stats_update}")
                    except queue.Empty:
                        continue
                    except Exception as e:
                        logger.error(f"Error in stats reader: {e}")
            
            threading.Thread(target=stats_reader, daemon=True).start()
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
            self.start()

            # Keep main process alive and monitor child processes
            logger = default_logger()            
            last_market_check = time.time()
            from PKDevTools.classes.GitHubSecrets import PKGitHubSecretsManager

            gh_manager = PKGitHubSecretsManager(repo="pkbrokers")
            gh_manager.test_encryption()
            test_mode_counter = 0
            while True:
                time.sleep(1)

                # Check if shutdown was requested (e.g., due to conflict)
                if self.shutdown_requested:
                    logger.warning(
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
                            target=PKTickOrchestrator.run_telegram_bot, 
                            args=(self.bot_token, self.ticks_file_path, self.chat_id, self.shared_stats,), 
                            name="PKTickBotProcess"
                        )
                        self.bot_process.daemon = False
                        self.bot_process.start()

                # Check for test mode request from bot
                if 'test_mode_requested' in self.shared_stats and self.shared_stats['test_mode_requested']:
                    logger.info("Test mode requested by bot")
                    self.test_mode = True
                    self.shared_stats['test_mode_requested'] = False # Reset the flag

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
                    # Check if we should commit pkl files (market close detection or periodic during trading)
                    try:
                        from pkbrokers.bot.dataSharingManager import get_data_sharing_manager
                        from pkbrokers.kite.inMemoryCandleStore import get_candle_store
                        from PKDevTools.classes.PKDateUtilities import PKDateUtilities
                        
                        data_mgr = get_data_sharing_manager()
                        
                        # Check for market close commit
                        if data_mgr.should_commit():
                            logger.info("Market close detected or recent data was received from previous running instance - committing pkl files")
                            candle_store = get_candle_store()
                            
                            # Export and commit daily pkl
                            data_mgr.export_daily_candles_to_pkl(candle_store)
                            
                            # Export and commit intraday pkl
                            data_mgr.export_intraday_candles_to_pkl(candle_store)
                            
                            # Commit to GitHub
                            data_mgr.commit_pkl_files()
                        
                        # Periodic commit during trading hours (every 30 minutes)
                        elif PKDateUtilities.isTradingTime():
                            cur_ist = PKDateUtilities.currentDateTime()
                            # Commit at minute 0 and 30 of each hour during trading
                            if cur_ist.minute in [0, 30] and cur_ist.second < 35:
                                last_commit = getattr(data_mgr, 'last_periodic_commit', None)
                                if last_commit is None or (cur_ist - last_commit).total_seconds() > 1500:  # 25 min gap
                                    logger.info(f"Periodic commit during trading hours ({cur_ist.hour}:{cur_ist.minute:02d})")
                                    candle_store = get_candle_store()
                                    
                                    # Export pkl files with current aggregated data
                                    data_mgr.export_daily_candles_to_pkl(candle_store, merge_with_historical=True)
                                    data_mgr.export_intraday_candles_to_pkl(candle_store)
                                    
                                    # Commit to GitHub
                                    data_mgr.commit_pkl_files()
                                    data_mgr.last_periodic_commit = cur_ist
                            
                    except Exception as commit_e:
                        logger.debug(f"Pkl commit check: {commit_e}")
                    
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

                        logger.debug(
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
                                    from pkbrokers.kite.examples.externals import kite_auth

                                    kite_auth()
                                    self.token_generated_at_least_once = True
                        except Exception as e:
                            logger.error(f"Error while updating token: {e}")

            self.stop()

        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt received")
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
        from PKDevTools.classes.log import default_logger
        logger = default_logger()
        logger.warning(f"Received signal {signum}. Shutting down gracefully...")
        self.shutdown_requested = True

    def get_consumer(self):
        """Get a consumer instance to interact with the bot"""
        from PKDevTools.classes import Archiver
        from PKDevTools.classes.Environment import PKEnvironment

        env = PKEnvironment()
        bot_token = self.bot_token or env.TBTOKEN
        bridge_bot_token = self.bridge_bot_token or env.BBTOKEN
        chat_id = self.chat_id or env.CHAT_ID
        
        from pkbrokers.bot.consumer import PKTickBotConsumer

        if not chat_id:
            raise ValueError("chat_id is required for consumer functionality")
        return PKTickBotConsumer(bot_token, bridge_bot_token, chat_id)


def orchestrate():
    # Initialize with None values, they will be set from environment when needed
    orchestrator = PKTickOrchestrator(None, None, None, None)
    
    # Try to get data from running instance before starting
    from PKDevTools.classes.log import default_logger
    from pkbrokers.kite.examples.pkkite import setupLogger
    setupLogger() # Ensure logger is set up before using default_logger
    logger = default_logger()
    logger.info("Attempting to request data from running PKTickBot instance...")
    
    try:
        from pkbrokers.bot.consumer import try_get_command_response_from_bot
        from pkbrokers.bot.dataSharingManager import get_data_sharing_manager
        
        data_mgr = get_data_sharing_manager()
        
        # Request data from running instance
        response = try_get_command_response_from_bot(command="/request_data")
        
        if response.get("success"):
            logger.info("Successfully received data from running instance")
            data_mgr.data_received_from_instance = True
            
            # Commit the received data immediately during market hours
            try:
                from PKDevTools.classes.PKDateUtilities import PKDateUtilities
                if PKDateUtilities.isTradingTime():
                    logger.info("Market is trading - committing received pkl files...")
                    from pkbrokers.kite.inMemoryCandleStore import get_candle_store
                    candle_store = get_candle_store()
                    
                    # Export and commit pkl files
                    data_mgr.export_daily_candles_to_pkl(candle_store, merge_with_historical=True)
                    data_mgr.export_intraday_candles_to_pkl(candle_store)
                    data_mgr.commit_pkl_files()
                    logger.info("Successfully committed received data to GitHub")
            except Exception as commit_e:
                logger.debug(f"Error committing received data: {commit_e}")
        else:
            logger.warning("No running instance or no data received, will try GitHub fallback")
            
            # Try GitHub fallback
            success_daily, daily_path = data_mgr.download_from_github(file_type="daily")
            success_intraday, intraday_path = data_mgr.download_from_github(file_type="intraday")
            
            if success_daily or success_intraday:
                logger.info("Downloaded data from GitHub actions-data-download branch")
                
                # Copy downloaded pkl files to results/Data for workflow to commit
                # This ensures the data is available even if candle store loading fails
                import shutil
                from datetime import datetime
                results_dir = os.path.join(os.getcwd(), "results", "Data")
                os.makedirs(results_dir, exist_ok=True)
                
                from PKDevTools.classes import Archiver
                _, file_name = Archiver.afterMarketStockDataExists()
                if file_name is not None and len(file_name) > 0:
                    today_suffix = file_name.replace(".pkl", "").replace("stock_data_", "")
                else:
                    today_suffix = datetime.now().strftime('%d%m%Y')
                
                if success_daily and daily_path and os.path.exists(daily_path):
                    # Copy as date-specific file
                    dest_daily = os.path.join(results_dir, f"stock_data_{today_suffix}.pkl")
                    shutil.copy(daily_path, dest_daily)
                    logger.info(f"Copied daily pkl to: {dest_daily}")
                    
                    # Also copy as generic name
                    shutil.copy(daily_path, os.path.join(results_dir, "daily_candles.pkl"))
                
                if success_intraday and intraday_path and os.path.exists(intraday_path):
                    dest_intraday = os.path.join(results_dir, f"intraday_stock_data_{today_suffix}.pkl")
                    shutil.copy(intraday_path, dest_intraday)
                    logger.info(f"Copied intraday pkl to: {dest_intraday}")
                    
                    shutil.copy(intraday_path, os.path.join(results_dir, "intraday_1m_candles.pkl"))
                
                # Load the downloaded pkl data into the candle store
                try:
                    from pkbrokers.kite.inMemoryCandleStore import get_candle_store
                    candle_store = get_candle_store()
                    
                    if success_daily and daily_path:
                        loaded = data_mgr.load_pkl_into_candle_store(daily_path, candle_store, interval='day')
                        logger.info(f"Loaded {loaded} instruments from daily pkl into candle store")
                    
                    if success_intraday and intraday_path:
                        loaded = data_mgr.load_pkl_into_candle_store(intraday_path, candle_store, interval='1m')
                        logger.info(f"Loaded {loaded} instruments from intraday pkl into candle store")
                        
                except Exception as load_err:
                    logger.warning(f"Error loading pkl into candle store: {load_err}")
            else:
                logger.warning("No fallback data available, starting fresh")
                
    except Exception as e:
        logger.warning(f"Could not get data from running instance or GitHub: {e}")
    
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
