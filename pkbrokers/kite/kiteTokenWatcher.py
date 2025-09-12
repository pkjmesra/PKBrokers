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

import json
import multiprocessing
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Empty, Queue

from PKDevTools.classes import Archiver, log
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKJoinableQueue import PKJoinableQueue

from pkbrokers.kite.instruments import KiteInstruments
from pkbrokers.kite.zerodhaWebSocketClient import ZerodhaWebSocketClient

# Optimal batch size depends on your tick frequency
OPTIMAL_TOKEN_BATCH_SIZE = 500  # Zerodha allows max 500 instruments in one batch
OPTIMAL_BATCH_TICK_WAIT_TIME_SEC = 5
DB_PROCESS_SPIN_OFF_WAIT_TIME_SEC = 0.5
JSON_PROCESS_SPIN_OFF_WAIT_TIME_SEC = 1
OPTIMAL_MAX_QUEUE_SIZE = 10000
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

# macOS fork safety
if sys.platform.startswith("darwin"):
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

# Set spawn context globally
multiprocessing.set_start_method(
    "spawn" if sys.platform.startswith("darwin") else "spawn", force=True
)


class JSONFileWriter:
    """Multiprocessing process to write ticks to JSON file with instrument_token as primary key"""

    def __init__(
        self, json_file_path, max_queue_size=OPTIMAL_MAX_QUEUE_SIZE, log_level=0
    ):
        self.json_file_path = json_file_path
        self.mp_context = multiprocessing.get_context(
            "spawn" if sys.platform.startswith("darwin") else "spawn"
        )
        self.data_queue = PKJoinableQueue(maxsize=max_queue_size, ctx=self.mp_context)
        self.stop_event = self.mp_context.Event()
        self.process = None
        self.kite_instruments = {}
        self.log_level = log_level
        self.setupLogger()
        self.logger = default_logger()

    def start(self, kite_instruments={}):
        """Start the JSON writer process"""
        self.process = self.mp_context.Process(target=self._writer_loop)
        self.process.daemon = True
        self.kite_instruments = kite_instruments
        self.process.start()

    def setupLogger(self):
        if self.log_level > 0:
            os.environ["PKDevTools_Default_Log_Level"] = str(self.log_level)
        log.setup_custom_logger(
            "pkbrokersDB",
            self.log_level,
            trace=False,
            log_file_path="PKBrokers-DBlog.txt",
            filter=None,
        )

    def _writer_loop(self):
        """Main writer loop running in separate process"""

        self.setupLogger()
        self.logger = default_logger()
        self.logger.info(f"JSON file writer started for {self.json_file_path}")
        # Load existing data if file exists
        data = defaultdict(dict)
        if os.path.exists(self.json_file_path):
            try:
                with open(self.json_file_path, "r") as f:
                    data.update(json.load(f))
                self.logger.info(
                    f"Loaded existing data from {self.json_file_path} with {len(data.keys())} instruments."
                )
            except Exception as e:
                self.logger.error(f"Error loading JSON file: {e}")

        last_save_time = time.time()
        save_interval = 5  # Save to file every 5 seconds

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # Process all available ticks in the queue
                processed_count = 0
                while True:
                    try:
                        tick_data = self.data_queue.get_nowait()
                        self._update_instrument_data(data, tick_data)
                        processed_count += 1
                    except Empty:
                        break

                # Save to file periodically
                current_time = time.time()
                if current_time - last_save_time >= save_interval:
                    self._save_to_file(data)
                    last_save_time = current_time

                    if processed_count > 0:
                        self.logger.debug(
                            f"JSON writer processed {processed_count} ticks, total instruments: {len(data)}"
                        )

                # Small sleep to prevent CPU spinning
                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"JSON writer error: {e}")
                time.sleep(1)

        # Final save
        self._save_to_file(data)
        self.logger.warn("JSON writer process stopped")

    def _update_instrument_data(self, data, tick_data):
        """Update instrument data with latest tick information"""
        instrument_token = tick_data["instrument_token"]

        if instrument_token not in data:
            # Initialize new instrument entry
            try:
                trading_symbol = "NA"
                trading_symbol = self.kite_instruments[instrument_token].tradingsymbol
            except BaseException:
                if instrument_token in NIFTY_50:
                    trading_symbol = "NIFTY 50"
                elif instrument_token in BSE_SENSEX:
                    trading_symbol = "SENSEX"
                pass
            data[instrument_token] = {
                "instrument_token": instrument_token,
                "trading_symbol": trading_symbol,
                "ohlcv": {
                    "open": tick_data["open_price"],
                    "high": tick_data["high_price"],
                    "low": tick_data["low_price"],
                    "close": tick_data["last_price"],
                    "volume": tick_data.get("day_volume", 0),
                    "timestamp": tick_data["timestamp"].isoformat()
                    if hasattr(tick_data["timestamp"], "isoformat")
                    else (tick_data["timestamp"] if "+" in tick_data["timestamp"] else f'{tick_data["timestamp"]}+05:30'),
                },
                "prev_day_close": tick_data["prev_day_close"],
                "buy_quantity": tick_data["buy_quantity"],
                "sell_quantity": tick_data["sell_quantity"],
                "oi": tick_data["oi"],
                "market_depth": tick_data.get("depth", {"bid": [], "ask": []}),
                "last_updated": datetime.now().isoformat(),
                "tick_count": 0,
            }

        # Update OHLCV
        current_ohlcv = data[instrument_token]["ohlcv"]
        current_price = tick_data["last_price"]

        # Update high and low
        if current_price > current_ohlcv["high"]:
            current_ohlcv["high"] = current_price
        if current_price < current_ohlcv["low"]:
            current_ohlcv["low"] = current_price

        # Update close and volume
        current_ohlcv["close"] = current_price
        current_ohlcv["volume"] = tick_data.get("day_volume", 0)
        current_ohlcv["timestamp"] = (
            tick_data["timestamp"].isoformat()
            if hasattr(tick_data["timestamp"], "isoformat")
            else tick_data["timestamp"]
        )

        # Update OI, buy_quantity, sell_quantity, prev_day_close
        data[instrument_token]["oi"] = tick_data["oi"]
        data[instrument_token]["buy_quantity"] = tick_data["buy_quantity"]
        data[instrument_token]["sell_quantity"] = tick_data["sell_quantity"]
        data[instrument_token]["prev_day_close"] = tick_data["prev_day_close"]

        # Update market depth (always use latest depth)
        if "depth" in tick_data and tick_data["depth"]:
            data[instrument_token]["market_depth"] = tick_data["depth"]

        # Update metadata
        data[instrument_token]["last_updated"] = datetime.now().isoformat()
        data[instrument_token]["tick_count"] += 1

    def _save_to_file(self, data):
        """Save data to JSON file atomically"""
        try:
            # Write to temporary file first
            temp_file = self.json_file_path + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(dict(data), f, indent=2, default=str)

            # Atomically replace the original file
            os.replace(temp_file, self.json_file_path)

        except Exception as e:
            self.logger.error(f"Error saving JSON file: {e}")

    def add_tick(self, tick_data):
        """Add tick data to the write queue"""
        try:
            self.data_queue.put(tick_data, timeout=0.1)
            return True
        except Exception:
            self.logger.warn("JSON writer queue full, dropping tick")
            return False

    def stop(self):
        """Stop the JSON writer"""
        self.stop_event.set()
        if self.process and self.process.is_alive():
            self.process.join(timeout=5)
        self.logger.info("JSON writer stopped")


class KiteTokenWatcher:
    """
    A high-performance tick data watcher and processor for Zerodha Kite Connect API.
    Now includes JSON file writing capability alongside database operations.

    This class manages real-time market data streaming with guaranteed:
    1. Exactly one tick per instrument_token in each batch (latest tick only)
    2. Batch processing every 30 seconds (configurable via OPTIMAL_BATCH_TICK_WAIT_TIME_SEC)
    3. Efficient database operations with proper error handling

    CRITICAL DESIGN FEATURES:
    - Uses dictionary for _tick_batch to ensure only latest tick per instrument is stored
    - Fixed-interval timing logic for consistent 30-second processing cycles
    - Simplified processing pipeline without unnecessary buffering
    - Comprehensive error handling throughout the data flow

    Attributes:
        _watcher_queue (Queue): Queue for receiving raw ticks from WebSocket
        _db_queue (Queue): Queue for processed batches ready for database insertion
        _processing_thread (Thread): Thread for processing raw ticks
        _db_thread (Thread): Thread for database operations
        _shutdown_event (Event): Event signal for graceful shutdown
        token_batches (list): List of token batches for WebSocket subscription
        client (ZerodhaWebSocketClient): WebSocket client instance
        logger (Logger): Logger instance for debugging and monitoring
        _db_instance (ThreadSafeDatabase): Database connection instance
        _tick_batch (dict): Dictionary storing only the latest tick for each instrument
        _next_process_time (datetime): Next scheduled batch processing time

    Example:
        >>> watcher = KiteTokenWatcher(tokens=[256265, 265])
        >>> watcher.watch()  # Starts watching with 30-second batch intervals
        >>> watcher.stop()   # Graceful shutdown
    """

    def __init__(
        self, tokens=[], watcher_queue=None, client=None, json_output_path=None
    ):
        """
        Initialize the KiteTokenWatcher instance.

        Args:
            tokens (list): List of instrument tokens to watch. If empty, fetches all equities.
            watcher_queue (Queue): Custom queue for tick data. Creates default if not provided.
            client (ZerodhaWebSocketClient): Pre-configured WebSocket client.
            json_output_path (str): Path for JSON output file. If None, uses default.

        CRITICAL: _tick_batch is a dictionary, not defaultdict(list), ensuring only
        one tick per instrument_token by design (key overwrites on new ticks).
        """
        self._watcher_queue = watcher_queue or Queue(maxsize=0)
        self._db_queue = Queue(maxsize=0)
        self._processing_thread = None
        self._db_thread = None
        self._shutdown_event = threading.Event()
        self._stop_queue = None
        self._stop_listener_thread = None
        self.log_level = (
            0
            if "PKDevTools_Default_Log_Level" not in os.environ.keys()
            else int(os.environ["PKDevTools_Default_Log_Level"])
        )

        # Split tokens into batches of max 500 (Zerodha limit)
        self.token_batches = [
            tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
            for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
        ]

        self.client = client
        self.logger = default_logger()
        self._db_instance = None

        # JSON file writer
        self.json_output_path = json_output_path or os.path.join(
            Archiver.get_user_data_dir(), "ticks.json"
        )

        self.json_writer = JSONFileWriter(
            json_file_path=self.json_output_path, log_level=self.log_level
        )
        self.json_writer.setupLogger()

        # CRITICAL: Using dictionary instead of defaultdict(list) ensures only
        # the latest tick for each instrument_token is stored (key overwrite behavior)
        self._tick_batch = {}

        self._next_process_time = None

    def set_stop_queue(self, stop_queue):
        """
        Set a queue to listen for stop signals from parent process

        Args:
            stop_queue: multiprocessing.Queue instance to listen for stop signals
        """
        self._stop_queue = stop_queue
        self._start_stop_listener()

    def _start_stop_listener(self):
        """Start a thread to listen for stop signals from the queue"""
        if self._stop_queue is None:
            return

        def listen_for_stop():
            while not self._shutdown_event.is_set():
                try:
                    # Check for stop signal with timeout to avoid blocking indefinitely
                    if self._stop_queue and not self._stop_queue.empty():
                        signal = self._stop_queue.get(timeout=0.1)
                        if signal == "STOP":
                            self.logger.info(
                                "Received stop signal from launcher/orchestrator"
                            )
                            self.stop()
                            break
                    time.sleep(0.1)
                except Empty:
                    continue
                except Exception as e:
                    self.logger.error(f"Error in stop listener: {e}")
                    break

        self._stop_listener_thread = threading.Thread(
            target=listen_for_stop, daemon=True, name="StopListener"
        )
        self._stop_listener_thread.start()
        self.logger.debug("Started stop signal listener thread")

    def watch(self, test_mode=False):
        """
        Start watching market data for configured tokens.

        This method:
        1. Fetches tokens if not provided during initialization
        2. Initializes WebSocket client if not provided
        3. Starts processing and database threads
        4. Begins WebSocket connection with 30-second batch intervals

        Raises:
            Exception: If WebSocket connection fails or token fetch fails
        """
        local_secrets = PKEnvironment().allSecrets
        self._db_instance = self._get_database()

        # Auto-fetch tokens if none provided
        if len(self.token_batches) == 0:
            API_KEY = "kitefront"
            ACCESS_TOKEN = os.environ.get(
                "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
            )
            kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)

            if kite.get_instrument_count() == 0:
                kite.sync_instruments(force_fetch=True)
            instruments = kite.fetch_instruments()
            # Start JSON writer first
            self.json_writer.start(
                kite_instruments=kite.kite_instruments if len(instruments) > 0 else {}
            )
            time.sleep(
                JSON_PROCESS_SPIN_OFF_WAIT_TIME_SEC
            )  # Let JSON writer initialize

            equities = kite.get_equities(column_names="instrument_token")
            tokens = kite.get_instrument_tokens(equities=equities)
            tokens = list(set(NIFTY_50 + BSE_SENSEX + tokens))

            self.token_batches = [
                tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
                for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
            ]

        self.logger.debug(
            f"Fetched {len(tokens)} tokens. Divided into {len(self.token_batches)} batches."
        )

        # Initialize WebSocket client if not provided
        if self.client is None:
            self.client = ZerodhaWebSocketClient(
                enctoken=os.environ.get(
                    "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
                ),
                user_id=os.environ.get(
                    "KUSER", local_secrets.get("KUSER", "You need your Kite user")
                ),
                token_batches=self.token_batches,
                watcher_queue=self._watcher_queue,
                db_conn=self._db_instance,
            )

        try:
            self._db_thread = threading.Thread(
                target=self._process_db_operations, daemon=True, name="DBProcessor"
            )
            self._db_thread.start()
            time.sleep(
                DB_PROCESS_SPIN_OFF_WAIT_TIME_SEC
            )  # Let's give time to the DB processes to get started
            # Start processing threads
            self._processing_thread = threading.Thread(
                target=self._process_ticks, daemon=True, name="TickProcessor"
            )
            self._processing_thread.start()

            self.logger.debug("Started tick processing and database threads")
            self.client.start()

        except KeyboardInterrupt:
            self.logger.warn("Keyboard interrupt received, shutting down...")
            self.stop()
        except Exception as e:
            self.logger.error(f"Error in client: {e}")
            self.stop()

    def _get_database(self):
        """
        Get or create the thread-safe database instance.

        Returns:
            ThreadSafeDatabase: Database instance for tick storage

        Note: Uses lazy initialization to avoid unnecessary database connections
        """
        if PKEnvironment().DB_TICKS and int(PKEnvironment().DB_TICKS) > 0:
            if self._db_instance is None:
                from pkbrokers.kite.threadSafeDatabase import ThreadSafeDatabase

                self._db_instance = ThreadSafeDatabase()
            return self._db_instance
        return None

    def _process_tick_batch(self, tick_batch):
        """
        Process a batch of ticks for all instruments with full OHLCV and depth processing.

        Args:
            tick_batch (dict): Dictionary mapping instrument tokens to their latest ticks

        CRITICAL: This method expects each instrument_token to have exactly one tick
        (the latest), ensuring no duplicates in the final database insert.
        """
        if not tick_batch:
            return

        processed_batch = []
        total_instruments = len(tick_batch)
        self.logger.info(
            f"Processing batch with {total_instruments} unique instruments"
        )

        for instrument_token, ticks in tick_batch.items():
            if not ticks:
                continue

            # CRITICAL: We only have one tick per instrument (the latest)
            latest_tick = ticks[0]  # Single tick in list format
            timestamp = datetime.fromtimestamp(latest_tick.exchange_timestamp)

            # Process market depth data
            depth_data = self._extract_depth(latest_tick)

            processed = {
                "instrument_token": latest_tick.instrument_token,
                "timestamp": timestamp,
                "last_price": latest_tick.last_price or 0,
                "day_volume": latest_tick.day_volume or 0,
                "oi": latest_tick.oi or 0,
                "buy_quantity": latest_tick.buy_quantity or 0,
                "sell_quantity": latest_tick.sell_quantity or 0,
                "high_price": latest_tick.high_price or 0,
                "low_price": latest_tick.low_price or 0,
                "open_price": latest_tick.open_price or 0,
                "prev_day_close": latest_tick.prev_day_close or 0,
                "depth": depth_data,
            }
            processed_batch.append(processed)

            # Send to JSON writer
            try:
                self.json_writer.add_tick(processed)
            except Exception as e:
                self.logger.error(f"Error sending to JSON writer: {e}")

        # Insert into database
        try:
            db = self._get_database()
            if db:
                db.insert_ticks(processed_batch)
                self.logger.info(
                    f"Successfully added {len(processed_batch)} records to database queue"
                )
        except Exception as e:
            self.logger.error(f"Error inserting to database: {e}")

    def _process_ticks(self):
        """
        Main processing thread method for handling incoming ticks.

        CRITICAL FEATURES:
        1. Uses dictionary for _tick_batch ensuring only latest tick per instrument
        2. Fixed 30-second interval processing using absolute time calculations
        3. Graceful shutdown handling with proper cleanup

        TIMING MECHANISM:
        - Sets _next_process_time to current time + 30 seconds initially
        - After each processing, resets _next_process_time to current time + 30 seconds
        - This ensures consistent 30-second intervals regardless of processing time
        """
        from pkbrokers.kite.ticks import Tick

        # CRITICAL: Set initial processing time to now + 30 seconds for exact intervals
        self._next_process_time = datetime.now() + timedelta(
            seconds=OPTIMAL_BATCH_TICK_WAIT_TIME_SEC
        )
        self.logger.debug(f"Initial processing time set to: {self._next_process_time}")

        while not self._shutdown_event.is_set():
            try:
                # Get tick with timeout to allow periodic checking
                try:
                    tick = self._watcher_queue.get(timeout=1)
                except Empty:
                    tick = None
                except Exception as e:
                    self.logger.error(f"Tick retrieval error: {e}")
                    tick = None

                current_time = datetime.now()

                # CRITICAL: Process batch every 30 seconds using absolute time comparison
                if current_time >= self._next_process_time:
                    processing_start = datetime.now()

                    if self._tick_batch:
                        # Convert to list format expected by downstream processing
                        batch_to_process = {
                            token: [tick] for token, tick in self._tick_batch.items()
                        }
                        self._db_queue.put(batch_to_process)
                        self.logger.info(
                            f"Queued {len(self._tick_batch)} instruments for processing"
                        )
                        self._tick_batch.clear()

                    # CRITICAL: Reset timer to current time + 30 seconds for exact interval
                    self._next_process_time = datetime.now() + timedelta(
                        seconds=OPTIMAL_BATCH_TICK_WAIT_TIME_SEC
                    )
                    processing_time = (
                        datetime.now() - processing_start
                    ).total_seconds()

                    self.logger.debug(
                        f"Batch processed in {processing_time:.2f}s. "
                        f"Next process time: {self._next_process_time}"
                    )

                # Process incoming tick if available
                if tick is None:
                    continue

                if isinstance(tick, Tick):
                    # CRITICAL: Dictionary assignment ensures only latest tick is kept
                    # Older ticks for same instrument are automatically replaced
                    self._tick_batch[tick.instrument_token] = tick
                    self._watcher_queue.task_done()

                    self.logger.debug(
                        f"Updated latest tick for instrument {tick.instrument_token}"
                    )

            except KeyboardInterrupt:
                self.logger.warn("Keyboard interrupt received in processing thread")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in tick processing: {e}")
                continue

        # Cleanup on shutdown
        self._cleanup_processing()

    def _cleanup_processing(self):
        """Handle graceful shutdown with proper cleanup of remaining data."""
        if self._tick_batch:
            batch_dict = {token: [tick] for token, tick in self._tick_batch.items()}
            self._db_queue.put(batch_dict)
            self.logger.info(
                f"Processed {len(batch_dict)} final instruments on shutdown"
            )

        self._db_queue.put(None)  # Signal database thread to exit
        self.logger.warn("Exiting tick processing thread")

    def _process_db_operations(self):
        """
        Dedicated database thread that processes batches from the queue.

        This thread:
        - Processes batches immediately as they arrive
        - Uses the full _process_tick_batch method for comprehensive processing
        - Includes robust error handling with fallback mechanisms
        - Ensures no batch loss during processing
        """
        self.logger.debug("Database processing thread started")

        while not self._shutdown_event.is_set():
            try:
                # Get batch with reasonable timeout
                batch = self._db_queue.get(timeout=2)

                if batch is None:  # Shutdown signal
                    self.logger.debug("Received shutdown signal in DB thread")
                    break

                # Process batch immediately using full processing method
                try:
                    self._process_tick_batch(batch)
                except Exception as e:
                    self.logger.error(f"Full processing failed, using fallback: {e}")
                    # Fallback to simple processing if full processing fails
                    self._process_batch_fallback(batch)

            except Empty:
                # Normal timeout, continue waiting
                continue
            except Exception as e:
                self.logger.error(f"Database thread error: {e}")
                continue

        self.logger.warn("Exiting database processing thread")

    def _process_batch_fallback(self, tick_batch):
        """
        Fallback processing method when full processing fails.

        Args:
            tick_batch (dict): Batch to process with simplified logic
        """
        try:
            processed_batch = self._prepare_batch_for_insertion(tick_batch)
            if processed_batch:
                db = self._get_database()
                if db:
                    db.insert_ticks(processed_batch)
                    self.logger.info(f"Fallback inserted {len(processed_batch)} records")
        except Exception as e:
            self.logger.error(f"Fallback processing also failed: {e}")

    def _prepare_batch_for_insertion(self, tick_batch):
        """
        Simplified batch preparation for database insertion.

        Args:
            tick_batch (dict): Dictionary of instrument tokens to ticks

        Returns:
            list: Processed data ready for database insertion

        Note: This is a fallback method and doesn't include full OHLCV processing
        """
        processed_batch = []

        for instrument_token, ticks in tick_batch.items():
            if not ticks:
                continue

            latest_tick = ticks[0]  # Single tick in list
            timestamp = datetime.fromtimestamp(latest_tick.exchange_timestamp)

            processed = {
                "instrument_token": latest_tick.instrument_token,
                "timestamp": timestamp,
                "last_price": latest_tick.last_price or 0,
                "day_volume": latest_tick.day_volume or 0,
                "oi": latest_tick.oi or 0,
                "buy_quantity": latest_tick.buy_quantity or 0,
                "sell_quantity": latest_tick.sell_quantity or 0,
                "high_price": latest_tick.high_price or 0,
                "low_price": latest_tick.low_price or 0,
                "open_price": latest_tick.open_price or 0,
                "prev_day_close": latest_tick.prev_day_close or 0,
                "depth": self._extract_depth(latest_tick)
                if hasattr(latest_tick, "depth")
                else {},
            }
            processed_batch.append(processed)

        return processed_batch

    def _extract_depth(self, tick):
        """
        Extract market depth data from a tick.

        Args:
            tick: The tick object containing depth information

        Returns:
            dict: Market depth data with bid/ask information
        """
        depth = {"bid": [], "ask": []}

        if not hasattr(tick, "depth"):
            return depth

        for i in range(1, 6):
            # Process bids
            bid_price = getattr(tick.depth, f"buy_{i}_price", 0)
            bid_quantity = getattr(tick.depth, f"buy_{i}_quantity", 0)
            bid_orders = getattr(tick.depth, f"buy_{i}_orders", 0)

            if bid_price and bid_quantity:
                depth["bid"].append(
                    {
                        "price": bid_price,
                        "quantity": bid_quantity,
                        "orders": bid_orders,
                    }
                )

            # Process asks
            ask_price = getattr(tick.depth, f"sell_{i}_price", 0)
            ask_quantity = getattr(tick.depth, f"sell_{i}_quantity", 0)
            ask_orders = getattr(tick.depth, f"sell_{i}_orders", 0)

            if ask_price and ask_quantity:
                depth["ask"].append(
                    {
                        "price": ask_price,
                        "quantity": ask_quantity,
                        "orders": ask_orders,
                    }
                )

        return depth

    def stop(self):
        """
        Graceful shutdown of all components.

        This method ensures:
        - Proper signaling to all threads
        - Cleanup of remaining data
        - Timeout-based thread termination
        - Resource cleanup
        """
        self.logger.info("Initiating graceful shutdown...")

        # Signal shutdown to all components
        self._shutdown_event.set()

        # Stop JSON writer
        if self.json_writer:
            self.json_writer.stop()

        # Stop WebSocket client
        if self.client:
            try:
                self.client.stop()
            except Exception as e:
                self.logger.error(f"Error stopping client: {e}")

        # Signal database thread to exit
        try:
            self._db_queue.put(None, timeout=2.0)
        except Exception:
            pass  # Queue might be full

        # Wait for threads with reasonable timeouts
        thread_timeout = 10.0

        if self._processing_thread and self._processing_thread.is_alive():
            self._processing_thread.join(timeout=thread_timeout)
            if self._processing_thread.is_alive():
                self.logger.warn("Processing thread did not terminate gracefully")

        if self._db_thread and self._db_thread.is_alive():
            self._db_thread.join(timeout=thread_timeout)
            if self._db_thread.is_alive():
                self.logger.warn("Database thread did not terminate gracefully")

        self.logger.info("Shutdown complete")

    def __del__(self):
        """
        Ensure cleanup on object destruction.

        Serves as safety net for resource cleanup if stop() wasn't called.
        """
        if not self._shutdown_event.is_set():
            self.logger.debug("Auto-cleanup in destructor")
            self.stop()
