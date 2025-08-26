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
from collections import defaultdict
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
    def __init__(self, tokens=[], watcher_queue=None, client=None):
        self._watcher_queue = watcher_queue or Queue(maxsize=10000)
        self._db_queue = Queue(maxsize=10000)
        self._processing_thread = None
        self._db_thread = None
        self._shutdown_event = threading.Event()  # Event for graceful shutdown
        # Split into batches of OPTIMAL_TOKEN_BATCH_SIZE (Zerodha's recommended chunk size)
        self.token_batches = [
            tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
            for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
        ]
        self.client = client
        self.logger = default_logger()
        self._db_instance = None
        self._tick_batch = defaultdict(list)
        self._next_process_time = None

    def watch(self):
        local_secrets = PKEnvironment().allSecrets
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
            tokens = list(set(NIFTY_50 + BSE_SENSEX + tokens))
            self.token_batches = [
                tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
                for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
            ]
        self.logger.debug(
            f"Fetched {len(tokens)} tokens. Divided into {len(self.token_batches)} batches."
        )
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
            # Start processing thread
            self._processing_thread = threading.Thread(
                target=self._process_ticks, daemon=True
            )
            self._processing_thread.start()
            self._db_thread = threading.Thread(
                target=self._process_db_operations, daemon=True
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
        if self._db_instance is None:
            from pkbrokers.kite.threadSafeDatabase import ThreadSafeDatabase

            self._db_instance = ThreadSafeDatabase()
        return self._db_instance

    def _process_tick_batch(self, tick_batch):
        """Process a batch of ticks for all instruments at once"""
        if not tick_batch:
            return

        processed_batch = []
        total_ticks_processed = 0

        for instrument_token, ticks in tick_batch.items():
            if not ticks:
                continue

            total_ticks_processed += len(ticks)

            # Get the most recent tick for this instrument
            latest_tick = ticks[-1]

            # Calculate OHLCV values from the batch
            high_prices = [
                tick.high_price for tick in ticks if tick.high_price is not None
            ]
            low_prices = [
                tick.low_price for tick in ticks if tick.low_price is not None
            ]
            volumes = [tick.day_volume for tick in ticks if tick.day_volume is not None]

            high_price = max(high_prices) if high_prices else 0
            low_price = min(low_prices) if low_prices else 0
            total_volume = sum(volumes) if volumes else 0

            # Convert exchange timestamp to proper datetime object for database
            timestamp = datetime.fromtimestamp(latest_tick.exchange_timestamp)

            # Prepare market depth data from the latest tick
            depth_data = {}
            if hasattr(latest_tick, "depth") and latest_tick.depth:
                depth_data = {"bid": [], "ask": []}

                # Process bids (positions 1-5)
                for i in range(1, 6):
                    bid_price = getattr(latest_tick.depth, f"buy_{i}_price", 0)
                    bid_quantity = getattr(latest_tick.depth, f"buy_{i}_quantity", 0)
                    bid_orders = getattr(latest_tick.depth, f"buy_{i}_orders", 0)

                    if bid_price and bid_quantity:
                        depth_data["bid"].append(
                            {
                                "price": bid_price,
                                "quantity": bid_quantity,
                                "orders": bid_orders,
                            }
                        )

                # Process asks (positions 1-5)
                for i in range(1, 6):
                    ask_price = getattr(latest_tick.depth, f"sell_{i}_price", 0)
                    ask_quantity = getattr(latest_tick.depth, f"sell_{i}_quantity", 0)
                    ask_orders = getattr(latest_tick.depth, f"sell_{i}_orders", 0)

                    if ask_price and ask_quantity:
                        depth_data["ask"].append(
                            {
                                "price": ask_price,
                                "quantity": ask_quantity,
                                "orders": ask_orders,
                            }
                        )

            processed = {
                "instrument_token": latest_tick.instrument_token,
                "timestamp": timestamp,  # Use datetime object instead of string
                "last_price": latest_tick.last_price
                if latest_tick.last_price is not None
                else 0,
                "day_volume": total_volume,
                "oi": latest_tick.oi if latest_tick.oi is not None else 0,
                "buy_quantity": latest_tick.buy_quantity
                if latest_tick.buy_quantity is not None
                else 0,
                "sell_quantity": latest_tick.sell_quantity
                if latest_tick.sell_quantity is not None
                else 0,
                "high_price": high_price,
                "low_price": low_price,
                "open_price": ticks[0].open_price
                if ticks[0].open_price is not None
                else 0,
                "prev_day_close": latest_tick.prev_day_close
                if latest_tick.prev_day_close is not None
                else 0,
                "depth": depth_data,
            }
            processed_batch.append(processed)

        # Process the entire batch
        current_time = datetime.now().strftime("%H:%M:%S")
        self.logger.debug(
            f"[{current_time}] Processed batch: {len(processed_batch)} instruments, {total_ticks_processed} total ticks"
        )

        # Insert into database
        try:
            db = self._get_database()
            db.insert_ticks(processed_batch)
            self.logger.info(
                f"Successfully inserted {len(processed_batch)} records to database"
            )

        except Exception as e:
            self.logger.debug(f"Error inserting to database: {e}")

    def _process_ticks(self):
        from pkbrokers.kite.ticks import Tick

        # Initialize next process time to the start of the next minute
        self._next_process_time = datetime.now().replace(
            second=0, microsecond=0
        ) + timedelta(seconds=OPTIMAL_BATCH_TICK_WAIT_TIME_SEC)

        # while self._watcher_queue is not None or (
        #     self._watcher_queue is not None and not self._watcher_queue.empty()
        # ):
        while not self._shutdown_event.is_set():
            try:
                # Get tick with timeout
                try:
                    tick = self._watcher_queue.get(timeout=1)
                except Empty:
                    tick = None
                    self.logger.debug("Queue timeout, no tick received")
                except BaseException as e:
                    tick = None
                    self.logger.error(f"Tick Error:{e}")

                current_time = datetime.now()

                # Check if it's time to process the batch
                if current_time >= self._next_process_time:
                    self.logger.debug(
                        f"Processing batch. Current time: {current_time}, Next process time: {self._next_process_time}"
                    )
                    if self._tick_batch:
                        batch_to_process = dict(self._tick_batch)
                        self._db_queue.put(batch_to_process)
                        self.logger.info(
                            f"Placing Ticks in DB Queue: {len(self._tick_batch)}"
                        )
                        self._tick_batch.clear()
                    # Schedule next processing for the next minute
                    self._next_process_time = self._next_process_time + timedelta(
                        seconds=OPTIMAL_BATCH_TICK_WAIT_TIME_SEC
                    )
                    self.logger.debug(
                        f"Next process time set to: {self._next_process_time}"
                    )
                else:
                    self.logger.debug(
                        f"Waiting {current_time} < {self._next_process_time}"
                    )

                if tick is None:
                    continue

                # Process the tick based on its type
                if isinstance(tick, Tick):
                    # Add to batch instead of processing immediately
                    self._tick_batch[tick.instrument_token].append(tick)
                    self._watcher_queue.task_done()
                    self.logger.debug(
                        f"Added tick to batch for instrument {tick.instrument_token}"
                    )

            except KeyboardInterrupt:
                self.logger.warn("Keyboard interrupt received")
                # Process any remaining ticks before exiting
                if self._tick_batch:
                    self._process_tick_batch(self._tick_batch)
                self._watcher_queue = None
                break
            except Exception as e:
                self.logger.debug(f"Error in tick processing: {e}")
                # Continue processing despite errors

        # Process any remaining ticks before exiting
        if self._tick_batch:
            self._db_queue.put(dict(self._tick_batch))
        self._db_queue.put(None)  # Signal database thread to exit
        self.logger.warn("Exiting tick processing...")

    def _process_db_operations(self):
        """Dedicated database thread with optimized inserts"""
        batch_buffer = []
        while not self._shutdown_event.is_set():
            try:
                # Get batch with timeout
                batch = self._db_queue.get(timeout=2)
                if batch is None:  # Shutdown signal
                    break

                batch_buffer.append(batch)

                # Process when we have enough batches or after timeout
                if len(batch_buffer) >= 3 or (
                    len(batch_buffer) > 0 and self._db_queue.empty()
                ):
                    self._process_buffered_batches(batch_buffer)
                    batch_buffer = []

            except Empty:
                # Process any buffered batches on timeout
                if batch_buffer:
                    self._process_buffered_batches(batch_buffer)
                    batch_buffer = []
            except KeyboardInterrupt:
                self.logger.debug("Keyboard interrupt received")
                # Process any remaining ticks before exiting
                if batch_buffer:
                    self._process_buffered_batches(batch_buffer)
                    batch_buffer = []
                break
            except Exception as e:
                self.logger.error(f"Database processing error: {e}")

    def _process_buffered_batches(self, batches):
        """Process multiple batches together for efficiency"""
        all_processed = []
        for batch in batches:
            processed = self._prepare_batch_for_insertion(batch)
            all_processed.extend(processed)

        if all_processed:
            db = self._get_database()
            db.insert_ticks(all_processed)
            self.logger.info(f"Inserted {len(all_processed)} records in bulk")

    def _prepare_batch_for_insertion(self, tick_batch):
        """Prepare batch data without the time-consuming processing"""
        processed_batch = []
        for instrument_token, ticks in tick_batch.items():
            if not ticks:
                continue

            latest_tick = ticks[-1]
            timestamp = datetime.fromtimestamp(latest_tick.exchange_timestamp)

            # Simplified processing - focus on essential data
            processed = {
                "instrument_token": latest_tick.instrument_token,
                "timestamp": timestamp,
                "last_price": latest_tick.last_price or 0,
                "day_volume": sum(t.day_volume or 0 for t in ticks),
                "oi": latest_tick.oi or 0,
                "buy_quantity": latest_tick.buy_quantity or 0,
                "sell_quantity": latest_tick.sell_quantity or 0,
                "high_price": max(t.high_price or 0 for t in ticks),
                "low_price": min(
                    t.low_price or 0 for t in ticks if t.low_price is not None
                )
                or 0,
                "open_price": ticks[0].open_price or 0 if ticks else 0,
                "prev_day_close": latest_tick.prev_day_close or 0,
                "depth": self._extract_depth(latest_tick)
                if hasattr(latest_tick, "depth")
                else {},
            }
            processed_batch.append(processed)

        return processed_batch

    def _extract_depth(self, tick):
        """Quick depth extraction"""
        depth = {"bid": [], "ask": []}
        for i in range(1, 6):
            if hasattr(tick.depth, f"buy_{i}_price"):
                depth["bid"].append(
                    {
                        "price": getattr(tick.depth, f"buy_{i}_price", 0),
                        "quantity": getattr(tick.depth, f"buy_{i}_quantity", 0),
                        "orders": getattr(tick.depth, f"buy_{i}_orders", 0),
                    }
                )
            if hasattr(tick.depth, f"sell_{i}_price"):
                depth["ask"].append(
                    {
                        "price": getattr(tick.depth, f"sell_{i}_price", 0),
                        "quantity": getattr(tick.depth, f"sell_{i}_quantity", 0),
                        "orders": getattr(tick.depth, f"sell_{i}_orders", 0),
                    }
                )
        return depth

    def stop(self):
        """Graceful shutdown of all components"""
        self.logger.debug("Initiating graceful shutdown...")

        # Set shutdown event
        self._shutdown_event.set()

        # Stop the client first
        if self.client:
            try:
                self.client.stop()
            except Exception as e:
                self.logger.error(f"Error stopping client: {e}")

        # Signal database thread to exit
        try:
            self._db_queue.put(None, timeout=2.0)
        except Exception:
            pass  # Queue might be full, but we tried

        # Wait for threads to finish with timeout
        if self._processing_thread:
            self._processing_thread.join(timeout=5.0)
        if self._db_thread:
            self._db_thread.join(timeout=10.0)  # Give DB thread more time

        self.logger.debug("Shutdown complete")

    def __del__(self):
        """Ensure cleanup on object destruction"""
        if not self._shutdown_event.is_set():
            self.stop()
