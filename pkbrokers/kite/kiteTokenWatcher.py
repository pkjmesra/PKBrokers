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
import threading
from datetime import datetime, timedelta
from queue import Empty, Queue

from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger

from pkbrokers.kite.instruments import KiteInstruments
from pkbrokers.kite.zerodhaWebSocketClient import ZerodhaWebSocketClient

# Optimal batch size depends on your tick frequency
OPTIMAL_TOKEN_BATCH_SIZE = 500  # Zerodha allows max 500 instruments in one batch
OPTIMAL_BATCH_TICK_WAIT_TIME_SEC = 30
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
    """
    A high-performance tick data watcher and processor for Zerodha Kite Connect API.

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

    def __init__(self, tokens=[], watcher_queue=None, client=None):
        """
        Initialize the KiteTokenWatcher instance.

        Args:
            tokens (list): List of instrument tokens to watch. If empty, fetches all equities.
            watcher_queue (Queue): Custom queue for tick data. Creates default if not provided.
            client (ZerodhaWebSocketClient): Pre-configured WebSocket client.

        CRITICAL: _tick_batch is a dictionary, not defaultdict(list), ensuring only
        one tick per instrument_token by design (key overwrites on new ticks).
        """
        self._watcher_queue = watcher_queue or Queue(maxsize=10000)
        self._db_queue = Queue(maxsize=10000)
        self._processing_thread = None
        self._db_thread = None
        self._shutdown_event = threading.Event()

        # Split tokens into batches of max 500 (Zerodha limit)
        self.token_batches = [
            tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
            for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
        ]

        self.client = client
        self.logger = default_logger()
        self._db_instance = None

        # CRITICAL: Using dictionary instead of defaultdict(list) ensures only
        # the latest tick for each instrument_token is stored (key overwrite behavior)
        self._tick_batch = {}

        self._next_process_time = None

    def watch(self):
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

        # Auto-fetch tokens if none provided
        if len(self.token_batches) == 0:
            API_KEY = "kitefront"
            ACCESS_TOKEN = os.environ.get(
                "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
            )
            kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)

            if kite.get_instrument_count() == 0:
                kite.sync_instruments(force_fetch=True)

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
            )

        try:
            # Start processing threads
            self._processing_thread = threading.Thread(
                target=self._process_ticks, daemon=True, name="TickProcessor"
            )
            self._processing_thread.start()

            self._db_thread = threading.Thread(
                target=self._process_db_operations, daemon=True, name="DBProcessor"
            )
            self._db_thread.start()

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
        if self._db_instance is None:
            from pkbrokers.kite.threadSafeDatabase import ThreadSafeDatabase

            self._db_instance = ThreadSafeDatabase()
        return self._db_instance

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

        # Insert into database
        try:
            db = self._get_database()
            db.insert_ticks(processed_batch)
            self.logger.info(
                f"Successfully inserted {len(processed_batch)} records to database"
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
                            f"Queued {len(self._tick_batch)} instruments for DB processing"
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
        self.logger.debug("Initiating graceful shutdown...")

        # Signal shutdown to all components
        self._shutdown_event.set()

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

        self.logger.debug("Shutdown complete")

    def __del__(self):
        """
        Ensure cleanup on object destruction.

        Serves as safety net for resource cleanup if stop() wasn't called.
        """
        if not self._shutdown_event.is_set():
            self.logger.debug("Auto-cleanup in destructor")
            self.stop()
