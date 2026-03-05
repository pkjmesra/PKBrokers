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
import pytz
import fcntl

# Define IST timezone once
IST = pytz.timezone('Asia/Kolkata')

from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Empty, Queue

from PKDevTools.classes import Archiver, log
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKJoinableQueue import PKJoinableQueue

from pkbrokers.kite.instruments import KiteInstruments
from pkbrokers.kite.zerodhaWebSocketClient import ZerodhaWebSocketClient
from pkbrokers.kite.inMemoryCandleStore import get_candle_store

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

def ensure_ist_datetime(dt_value):
        """
        Convert any datetime input to IST timezone-aware datetime
        """
        if dt_value is None:
            return datetime.now(IST)
        
        # If it's already a datetime object
        if hasattr(dt_value, 'tzinfo'):
            # If it has timezone, convert to IST
            if dt_value.tzinfo is not None:
                return dt_value.astimezone(IST)
            else:
                # If naive, assume UTC and convert to IST
                return pytz.UTC.localize(dt_value).astimezone(IST)
        
        # If it's a string, try to parse it
        if isinstance(dt_value, str):
            try:
                # Try parsing with fromisoformat
                dt_parsed = datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
                if dt_parsed.tzinfo is None:
                    dt_parsed = pytz.UTC.localize(dt_parsed)
                return dt_parsed.astimezone(IST)
            except (ValueError, TypeError):
                # If parsing fails, return current IST time
                return datetime.now(IST)
        
        # Default fallback
        return datetime.now(IST)
class JSONFileWriter:
    """Multiprocessing process to write ticks to JSON file with instrument_token as primary key"""

    def __init__(
        self, json_file_path, max_queue_size=OPTIMAL_MAX_QUEUE_SIZE, log_level=0, writer_id=None
    ):
        # Set spawn context globally
        multiprocessing.set_start_method(
            "spawn" if sys.platform.startswith("darwin") else "spawn", force=True
        )
        self.json_file_path = json_file_path
        self.writer_id = writer_id or f"writer_{os.getpid()}"
        self.lock_file = json_file_path + ".lock"
        self.mp_context = multiprocessing.get_context(
            "spawn" if sys.platform.startswith("darwin") else "spawn"
        )
        self.data_queue = PKJoinableQueue(maxsize=max_queue_size, ctx=self.mp_context)
        self.stop_event = self.mp_context.Event()
        self.process = None
        self.kite_instruments = {}
        self.log_level = log_level
        self._started = False
        self.setupLogger()
        self.logger = default_logger()

    @contextmanager
    def _file_lock(self):
        """
        Context manager for file locking using flock
        Ensures only one process writes to the file at a time
        """
        lock_fd = None
        try:
            # Open lock file for writing (creates if doesn't exist)
            lock_fd = open(self.lock_file, 'w')
            # Acquire exclusive lock (blocks until acquired)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            self.logger.debug(f"Lock acquired by {self.writer_id}")
            yield
        except Exception as e:
            self.logger.error(f"Error acquiring lock: {e}")
            raise
        finally:
            if lock_fd:
                try:
                    # Release lock
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    lock_fd.close()
                    self.logger.debug(f"Lock released by {self.writer_id}")
                except Exception as e:
                    self.logger.error(f"Error releasing lock: {e}")

    def _check_already_running(self):
        """Check if another writer process is already running using PID file"""
        pid_file = self.json_file_path + ".pid"
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                # Check if process with this PID is still running
                try:
                    os.kill(pid, 0)  # Signal 0 just checks if process exists
                    self.logger.warning(f"Another JSON writer process (PID: {pid}) is already running!")
                    return True
                except OSError:
                    # Process not running, safe to start
                    self.logger.info(f"Stale PID file found for PID {pid}, cleaning up")
                    os.unlink(pid_file)
            except (ValueError, IOError) as e:
                self.logger.error(f"Error reading PID file: {e}")
        return False

    def start(self, kite_instruments={}):
        """Start the JSON writer process only once"""
        if self._started:
            self.logger.warning("JSON writer already started, ignoring duplicate start")
            return
        
        # Check if another instance is already running
        if self._check_already_running():
            self.logger.error("Cannot start - another JSON writer instance is running")
            return
        
        self._started = True
        self.process = self.mp_context.Process(target=self._writer_loop)
        self.process.daemon = True
        self.kite_instruments = kite_instruments
        self.process.start()
        
        # Write PID file
        try:
            with open(self.json_file_path + ".pid", 'w') as f:
                f.write(str(self.process.pid))
        except Exception as e:
            self.logger.error(f"Error writing PID file: {e}")
        
        self.logger.info(f"JSON writer started with PID: {self.process.pid}")

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

    def _deduplicate_data(self, data):
        """
        Deduplicate data by keeping the latest entry for each instrument_token
        based on last_updated timestamp
        """
        clean_data = {}
        
        for token, instrument_data in data.items():
            token_str = str(token)
            
            if token_str in clean_data:
                # Compare last_updated timestamps
                existing = clean_data[token_str].get('last_updated', '1970-01-01T00:00:00+00:00')
                new = instrument_data.get('last_updated', '1970-01-01T00:00:00+00:00')
                
                # Parse and compare timestamps
                try:
                    # Handle ISO format with timezone
                    existing_dt = datetime.fromisoformat(existing.replace('Z', '+00:00'))
                    new_dt = datetime.fromisoformat(new.replace('Z', '+00:00'))
                    
                    if new_dt > existing_dt:
                        clean_data[token_str] = instrument_data
                        self.logger.debug(f"Updated {token_str} with newer data")
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Error comparing timestamps for {token_str}: {e}")
                    # If can't parse, keep the existing one
                    pass
            else:
                clean_data[token_str] = instrument_data
        
        return clean_data

    def _validate_json_structure(self, data):
        """
        Validate that data can be properly serialized to JSON and has no structural issues
        
        Returns:
            tuple: (is_valid, validated_data, error_message)
        """
        try:
            # Test serialization
            json_str = json.dumps(data, default=str)
            
            # Test that it can be parsed back
            parsed = json.loads(json_str)
            
            # Check for data loss during serialization
            if len(parsed) != len(data):
                self.logger.warning(f"Data loss detected: {len(data)} -> {len(parsed)}")
                return False, parsed, "Data loss during serialization"
            
            # Validate each instrument has required fields
            required_fields = ['instrument_token', 'trading_symbol', 'ohlcv', 'last_updated']
            for token, instrument in parsed.items():
                missing_fields = [f for f in required_fields if f not in instrument]
                if missing_fields:
                    self.logger.warning(f"Instrument {token} missing fields: {missing_fields}")
                    return False, parsed, f"Missing required fields: {missing_fields}"
                
                # Validate ohlcv has required fields
                ohlcv_fields = ['open', 'high', 'low', 'close', 'volume', 'timestamp']
                if 'ohlcv' in instrument:
                    missing_ohlcv = [f for f in ohlcv_fields if f not in instrument['ohlcv']]
                    if missing_ohlcv:
                        self.logger.warning(f"Instrument {token} missing OHLCV fields: {missing_ohlcv}")
                        return False, parsed, f"Missing OHLCV fields: {missing_ohlcv}"
            
            return True, parsed, None
            
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            self.logger.error(f"JSON validation failed: {e}")
            return False, None, str(e)

    def _write_atomic(self, data):
        """
        Write data to file atomically using a temporary file
        """
        temp_file = self.json_file_path + ".tmp." + self.writer_id
        
        try:
            # Write to temporary file
            with open(temp_file, "w", encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk
            
            # Verify the temp file was written correctly
            if not os.path.exists(temp_file) or os.path.getsize(temp_file) == 0:
                raise IOError("Temporary file write failed")
            
            # Atomically replace the original file
            os.replace(temp_file, self.json_file_path)
            
            self.logger.debug(f"Successfully wrote {len(data)} instruments to {self.json_file_path}")
            
        except Exception as e:
            self.logger.error(f"Error in atomic write: {e}")
            # Clean up temp file if it exists
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            except:
                pass
            raise

    def _save_to_file(self, data):
        """
        Save data to JSON file with:
        1. File locking to prevent concurrent writes
        2. Deduplication to ensure unique instrument tokens
        3. JSON validation to ensure data integrity
        4. Atomic writes to prevent corruption
        """
        temp_file = None
        try:
            # Step 1: Convert defaultdict to regular dict if needed
            if hasattr(data, 'default_factory'):
                data_dict = dict(data)
            else:
                data_dict = data
            
            self.logger.debug(f"Processing {len(data_dict)} raw entries")
            
            # Step 2: Deduplicate data
            deduped_data = self._deduplicate_data(data_dict)
            self.logger.debug(f"After deduplication: {len(deduped_data)} unique instruments")
            
            # Step 3: Validate JSON structure
            is_valid, validated_data, error_msg = self._validate_json_structure(deduped_data)
            
            if not is_valid:
                self.logger.error(f"Data validation failed: {error_msg}")
                if validated_data:
                    # Try to salvage by using the parsed data
                    self.logger.warning("Attempting to salvage by using parsed data")
                    validated_data = self._deduplicate_data(validated_data)
                else:
                    # Can't salvage, don't save
                    self.logger.error("Data is corrupted beyond repair, skipping save")
                    return
            
            # Step 4: Acquire lock and write atomically
            with self._file_lock():
                self.logger.debug("Lock acquired, proceeding with atomic write")
                self._write_atomic(validated_data)
            
            self.logger.info(f"✅ Successfully saved {len(validated_data)} unique instruments to {self.json_file_path}")
            
        except Exception as e:
            self.logger.error(f"❌ Error saving JSON file: {e}")
            # Clean up temp file if it exists
            try:
                if temp_file and os.path.exists(temp_file):
                    os.unlink(temp_file)
            except:
                pass

    def _writer_loop(self):
        """Main writer loop running in separate process"""
        self.setupLogger()
        self.logger = default_logger()
        self.logger.info(f"JSON file writer [{self.writer_id}] started for {self.json_file_path}")
        
        # Load existing data if file exists
        data = {}
        if os.path.exists(self.json_file_path):
            try:
                with open(self.json_file_path, "r") as f:
                    loaded_data = json.load(f)
                    data.update(loaded_data)
                self.logger.info(
                    f"Loaded existing data from {self.json_file_path} with {len(data.keys())} instruments."
                )
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error loading file: {e}")
                # Try to salvage by reading as text and finding valid JSON
                try:
                    with open(self.json_file_path, "r") as f:
                        content = f.read()
                    # Look for valid JSON structure
                    import re
                    match = re.search(r'\{.*\}', content, re.DOTALL)
                    if match:
                        salvage_data = json.loads(match.group())
                        data.update(salvage_data)
                        self.logger.info(f"Salvaged {len(data)} instruments from corrupted file")
                except:
                    self.logger.error("Could not salvage data from corrupted file, starting fresh")
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
        instrument_token = str(tick_data["instrument_token"])  # Ensure string key

        if instrument_token not in data:
            # Initialize new instrument entry
            try:
                trading_symbol = "NA"
                trading_symbol = self.kite_instruments[int(instrument_token)].tradingsymbol
            except BaseException:
                if int(instrument_token) in NIFTY_50:
                    trading_symbol = "NIFTY 50"
                elif int(instrument_token) in BSE_SENSEX:
                    trading_symbol = "SENSEX"
                pass
            data[instrument_token] = {
                "instrument_token": int(instrument_token),
                "trading_symbol": trading_symbol,
                "ohlcv": {
                    "open": tick_data["open_price"],
                    "high": tick_data["high_price"],
                    "low": tick_data["low_price"],
                    "close": tick_data["last_price"],
                    "volume": tick_data.get("day_volume", 0),
                    "timestamp": ensure_ist_datetime(tick_data["timestamp"]).isoformat(),
                },
                "prev_day_close": tick_data["prev_day_close"],
                "buy_quantity": tick_data["buy_quantity"],
                "sell_quantity": tick_data["sell_quantity"],
                "oi": tick_data["oi"],
                "market_depth": tick_data.get("depth", {"bid": [], "ask": []}),
                "last_updated": ensure_ist_datetime(tick_data.get("timestamp", datetime.now(IST))).isoformat(),
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
        current_ohlcv["timestamp"] = ensure_ist_datetime(tick_data["timestamp"]).isoformat()

        # Update OI, buy_quantity, sell_quantity, prev_day_close
        data[instrument_token]["oi"] = tick_data["oi"]
        data[instrument_token]["buy_quantity"] = tick_data["buy_quantity"]
        data[instrument_token]["sell_quantity"] = tick_data["sell_quantity"]
        data[instrument_token]["prev_day_close"] = tick_data["prev_day_close"]

        # Update market depth (always use latest depth)
        if "depth" in tick_data and tick_data["depth"]:
            data[instrument_token]["market_depth"] = tick_data["depth"]

        # Update metadata
        data[instrument_token]["last_updated"] = ensure_ist_datetime(tick_data.get("timestamp", datetime.now(IST))).isoformat()
        data[instrument_token]["tick_count"] += 1

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
        
        # Clean up PID file
        try:
            pid_file = self.json_file_path + ".pid"
            if os.path.exists(pid_file):
                os.unlink(pid_file)
        except Exception as e:
            self.logger.error(f"Error cleaning up PID file: {e}")
        
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
        self, tokens=[], watcher_queue=None, client=None, json_output_path=None, shared_stats: dict = None
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
        self.logger.info(f"KiteTokenWatcher.__init__ received shared_stats: {dict(shared_stats) if shared_stats else 'None'}")
        self.logger.info(f"shared_stats type: {type(shared_stats)}")

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
        self._last_processed_instruments = []
        self._next_process_log_time = None
        # Initialize in-memory candle store for high-performance candle access
        self._candle_store = get_candle_store(shared_stats=shared_stats)
        self._kite_instruments = {}
        self.ws_stop_event = None  # Add this for WebSocket stop signal
        self.shared_stats = shared_stats if shared_stats is not None else {}
        # Also update the shared_stats dictionary with initial values
        if self.shared_stats is not None:
            self.shared_stats['instrument_count'] = 0
            self.shared_stats['instruments_with_ticks'] = 0
            self.shared_stats['ticks_processed'] = 0
            self.shared_stats['uptime_seconds'] = 0
            self.shared_stats['candles_created'] = 0
            self.shared_stats['candles_completed'] = 0
            self.shared_stats['last_tick_time'] = 0
            self.shared_stats['start_time'] = time.time()
            
            try:
                self.shared_stats['watcher_initialized'] = True
                self.logger.info(f"Updated shared_stats in watcher: {dict(self.shared_stats)}")
            except Exception as e:
                self.logger.error(f"Could not update shared_stats: {e}")

    def set_ws_stop_event(self, event):
        """Set the stop event for WebSocket processes"""
        self.ws_stop_event = event
        self.logger.info(f"ws_stop_event set to: {event}")
        
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

            try:
                if kite.get_instrument_count() == 0:
                    kite.sync_instruments(force_fetch=True)
                instruments = kite.fetch_instruments()
                # Start JSON writer first
                self._kite_instruments = kite.kite_instruments if len(instruments) > 0 else {}
            except Exception as db_error:
                # Handle Turso database blocked/unavailable - use fallback
                self.logger.warning(f"Database unavailable, using fallback: {db_error}")
                self._kite_instruments = {}
                instruments = []
            
            self.json_writer.start(kite_instruments=self._kite_instruments)
            
            # Register instruments with candle store for symbol mapping
            for token, inst in self._kite_instruments.items():
                if hasattr(inst, 'tradingsymbol'):
                    self._candle_store.register_instrument(int(token), inst.tradingsymbol)
            time.sleep(
                JSON_PROCESS_SPIN_OFF_WAIT_TIME_SEC
            )  # Let JSON writer initialize

            try:
                equities = kite.get_equities(column_names="instrument_token")
                tokens = kite.get_instrument_tokens(equities=equities)
                self.logger.info(f"Got {len(tokens)} tokens from Turso DB")
            except Exception as db_error:
                # Fallback: Use cached instruments or fetch from Kite API directly
                self.logger.warning(f"Could not get equities from DB, using fallback: {db_error}")
                tokens = []
            
            # CRITICAL FIX: Fallback logic runs whenever tokens is empty (0)
            # This handles BOTH exception case AND when DB returns empty list gracefully
            if len(tokens) == 0:
                self.logger.info("No tokens from DB, applying fallback strategies...")
                
                # Fallback 0: First check if _kite_instruments was populated by sync_instruments
                # This happens when sync_instruments(force_fetch=True) fetches from Zerodha API
                if self._kite_instruments and len(self._kite_instruments) > 0:
                    tokens = [int(token) for token in self._kite_instruments.keys()]
                    self.logger.info(f"Using {len(tokens)} tokens from already-loaded kite_instruments")
                    # Instruments are already registered in candle store above, so skip fallbacks
                
                # Fallback 1: Fetch NSE equity symbols from PKScreener and map to Kite tokens
                if len(tokens) == 0:
                    try:
                        import pandas as pd
                        import requests
                        self.logger.info("Fetching NSE equity symbols from PKScreener GitHub...")
                        equity_csv_url = "https://raw.githubusercontent.com/pkjmesra/PKScreener/main/results/Indices/EQUITY_L.csv"
                        response = requests.get(equity_csv_url, timeout=30)
                        if response.status_code == 200:
                            from io import StringIO
                            equity_df = pd.read_csv(StringIO(response.text))
                            nse_symbols = equity_df['SYMBOL'].str.strip().tolist()
                            self.logger.info(f"Fetched {len(nse_symbols)} NSE symbols from PKScreener")
                            
                            # Now fetch Kite instruments to map symbols to tokens
                            kite_url = "https://api.kite.trade/instruments/NSE"
                            kite_response = requests.get(kite_url, timeout=60)
                            if kite_response.status_code == 200:
                                kite_df = pd.read_csv(StringIO(kite_response.text))
                                # Filter to only EQ segment and match with our NSE symbols
                                eq_df = kite_df[
                                    (kite_df['segment'] == 'NSE') & 
                                    (kite_df['tradingsymbol'].isin(nse_symbols))
                                ]
                                tokens = eq_df['instrument_token'].tolist()
                                self.logger.info(f"Mapped {len(tokens)} NSE symbols to Kite instrument tokens")
                                # Register instruments with candle store
                                for _, row in eq_df.iterrows():
                                    self._candle_store.register_instrument(
                                        int(row['instrument_token']),
                                        row.get('tradingsymbol', str(row['instrument_token']))
                                    )
                            else:
                                self.logger.warning(f"Kite instruments fetch failed: {kite_response.status_code}")
                        else:
                            self.logger.warning(f"PKScreener equity CSV fetch failed: {response.status_code}")
                    except Exception as pkscreener_error:
                        self.logger.warning(f"PKScreener fallback failed: {pkscreener_error}")
                
                # Fallback 2: Try InstrumentHistory API if PKScreener failed
                if len(tokens) == 0:
                    try:
                        from pkbrokers.kite.instrumentHistory import InstrumentHistory
                        hist = InstrumentHistory(access_token=ACCESS_TOKEN)
                        instruments_df = hist.get_instruments(exchange="NSE")
                        if instruments_df is not None and len(instruments_df) > 0:
                            tokens = instruments_df[instruments_df['segment'] == 'NSE']['instrument_token'].tolist()
                            self.logger.info(f"Fetched {len(tokens)} tokens from InstrumentHistory API")
                            for _, row in instruments_df[instruments_df['segment'] == 'NSE'].iterrows():
                                self._candle_store.register_instrument(
                                    int(row['instrument_token']), 
                                    row.get('tradingsymbol', str(row['instrument_token']))
                                )
                    except Exception as api_error:
                        self.logger.warning(f"InstrumentHistory failed: {api_error}")
                
                # Fallback 3: Fetch all instruments directly from Zerodha as last resort
                if len(tokens) == 0:
                    try:
                        import pandas as pd
                        import requests
                        self.logger.info("Fetching ALL instruments directly from Zerodha CSV...")
                        nse_url = "https://api.kite.trade/instruments/NSE"
                        response = requests.get(nse_url, timeout=60)
                        if response.status_code == 200:
                            from io import StringIO
                            df = pd.read_csv(StringIO(response.text))
                            # Filter for EQ (equity) segment only
                            eq_df = df[df['segment'] == 'NSE']
                            tokens = eq_df['instrument_token'].tolist()
                            self.logger.info(f"Fetched {len(tokens)} NSE tokens from Zerodha CSV")
                            for _, row in eq_df.iterrows():
                                self._candle_store.register_instrument(
                                    int(row['instrument_token']),
                                    row.get('tradingsymbol', str(row['instrument_token']))
                                )
                        else:
                            self.logger.warning(f"Zerodha CSV fetch failed with status {response.status_code}")
                    except Exception as csv_error:
                        self.logger.error(f"Could not fetch instruments from Zerodha CSV: {csv_error}")
            
            # Always include indices even if database is unavailable
            tokens = list(set(NIFTY_50 + BSE_SENSEX + OTHER_INDICES + tokens))
            
            # Log warning if we only have indices (no equities)
            if len(tokens) <= len(NIFTY_50 + BSE_SENSEX + OTHER_INDICES):
                self.logger.warning(
                    f"Only {len(tokens)} index tokens available. "
                    f"Database may be blocked. Ticks will be limited to indices only."
                )

            self.token_batches = [
                tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
                for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
            ]

        self.logger.debug(
            f"Fetched {len(tokens)} tokens. Divided into {len(self.token_batches)} batches."
        )

        # Initialize WebSocket client if not provided
        if self.client is None:
            self.logger.info(f"Creating WebSocket client with ws_stop_event: {self.ws_stop_event}")
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
                ws_stop_event=self.ws_stop_event,  # Pass the stop event to WebSocket client
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
            timestamp = datetime.fromtimestamp(latest_tick.last_trade_timestamp) if latest_tick.last_trade_timestamp else latest_tick.exchange_timestamp

            # Process market depth data
            depth_data = self._extract_depth(latest_tick)

            processed = {
                "instrument_token": latest_tick.instrument_token,
                "timestamp": ensure_ist_datetime(timestamp),
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
            
            # Update in-memory candle store for high-performance candle access
            try:
                trading_symbol = ""
                if instrument_token in self._kite_instruments:
                    trading_symbol = getattr(self._kite_instruments[instrument_token], 'tradingsymbol', '')
                
                tick_for_candle = {
                    'instrument_token': latest_tick.instrument_token,
                    'last_price': latest_tick.last_price or 0,
                    'day_volume': latest_tick.day_volume or 0,
                    'oi': latest_tick.oi or 0,
                    'exchange_timestamp': latest_tick.exchange_timestamp,
                    'trading_symbol': trading_symbol,
                    'type': 'tick',
                }
                self._candle_store.process_tick(tick_for_candle)
            except Exception as e:
                self.logger.debug(f"Error updating candle store: {e}")

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
        self._next_process_log_time = datetime.now() + timedelta(
            seconds=12*OPTIMAL_BATCH_TICK_WAIT_TIME_SEC
        )
        self.logger.debug(f"Initial processing time set to: {self._next_process_time}")
        self.logger.debug(f"Initial log time set to: {self._next_process_log_time}")

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
                    self._last_processed_instruments.append(str(tick.instrument_token))
                    if current_time >= self._next_process_log_time:
                        self._next_process_log_time = datetime.now() + timedelta(
                            seconds=12*OPTIMAL_BATCH_TICK_WAIT_TIME_SEC
                        )
                        self.logger.debug(
                            f"Updated latest tick for instruments {','.join(self._last_processed_instruments)}"
                        )
                        self._last_processed_instruments.clear()
                
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
                "timestamp": ensure_ist_datetime(timestamp),
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

    def get_candles(
        self,
        symbol: str = None,
        instrument_token: int = None,
        interval: str = '5m',
        count: int = 50,
    ):
        """
        Get candles for an instrument from the in-memory store.
        
        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            instrument_token: Instrument token (alternative to symbol)
            interval: Candle interval ('1m', '2m', '3m', '4m', '5m', '10m', '15m', '30m', '60m', 'day')
            count: Number of candles to return
            
        Returns:
            List of candle dictionaries
        """
        return self._candle_store.get_candles(
            instrument_token=instrument_token,
            trading_symbol=symbol,
            interval=interval,
            count=count,
        )
    
    def get_candles_df(
        self,
        symbol: str = None,
        instrument_token: int = None,
        interval: str = '5m',
        count: int = 50,
    ):
        """
        Get candles as a pandas DataFrame.
        
        Args:
            symbol: Trading symbol
            instrument_token: Instrument token
            interval: Candle interval
            count: Number of candles
            
        Returns:
            DataFrame with OHLCV columns
        """
        return self._candle_store.get_ohlcv_dataframe(
            instrument_token=instrument_token,
            trading_symbol=symbol,
            interval=interval,
            count=count,
        )
    
    def get_day_ohlcv(self, symbol: str = None, instrument_token: int = None):
        """Get today's OHLCV data for an instrument."""
        return self._candle_store.get_day_ohlcv(
            instrument_token=instrument_token,
            trading_symbol=symbol,
        )
    
    def get_all_day_ohlcv(self):
        """Get today's OHLCV for all instruments."""
        return self._candle_store.get_all_instruments_ohlcv(interval='day')
    
    def get_candle_store_stats(self):
        """Get statistics from the candle store."""
        return self._candle_store.get_stats()
    
    def export_candle_store(self, pickle_path: str = None, json_path: str = None):
        """
        Export candle store data to files.
        
        Args:
            pickle_path: Path for pickle file (optional)
            json_path: Path for ticks.json file (optional)
        """
        if json_path:
            self._candle_store.save_ticks_json(json_path)
        elif json_path is None:
            self._candle_store.save_ticks_json()
        
        if pickle_path:
            data = self._candle_store.export_to_pickle_format()
            import pickle
            with open(pickle_path, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            self.logger.info(f"Exported candle store to {pickle_path}")

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
        
        # Signal WebSocket processes to stop if we have the event
        if self.ws_stop_event:
            self.logger.info("Signaling WebSocket processes to stop...")
            self.ws_stop_event.set()
        
        # Stop WebSocket client first to stop receiving new ticks
        if self.client:
            try:
                self.client.stop()
                self.logger.info("WebSocket client stopped")
            except Exception as e:
                self.logger.error(f"Error stopping client: {e}")
        
        # Save candle store data before shutdown
        try:
            self._candle_store.save_ticks_json()
            self.logger.info("Saved candle store to ticks.json")
        except Exception as e:
            self.logger.error(f"Error saving candle store: {e}")
        
        # Export pkl files with candle data
        try:
            from pkbrokers.bot.dataSharingManager import DataSharingManager
            from datetime import datetime
            import os
            import shutil
            
            # Use results/Data directory relative to cwd for workflow compatibility
            results_dir = os.path.join(os.getcwd(), "results", "Data")
            os.makedirs(results_dir, exist_ok=True)
            
            # Create data manager with the correct directory
            data_mgr = DataSharingManager(data_dir=results_dir)
            
            # Export daily candles to pkl (with historical merge)
            self.logger.info("Starting daily pkl export with historical merge...")
            success_daily, daily_path = data_mgr.export_daily_candles_to_pkl(self._candle_store, merge_with_historical=True)
            _, file_name = Archiver.afterMarketStockDataExists()
            if file_name is not None and len(file_name) > 0:
                today_suffix = file_name.replace('.pkl','').replace('stock_data_','')
            else:
                today_suffix = datetime.now().strftime('%d%m%Y')
            
            if success_daily and daily_path:
                file_size = os.path.getsize(daily_path) / (1024 * 1024)
                self.logger.info(f"Exported daily candles to: {daily_path} ({file_size:.2f} MB)")
                
                # Also create date-suffixed copy
                dest_daily = os.path.join(results_dir, f"stock_data_{today_suffix}.pkl")
                if daily_path != dest_daily:
                    shutil.copy(daily_path, dest_daily)
                    self.logger.info(f"Created date-suffixed copy: stock_data_{today_suffix}.pkl")
            else:
                self.logger.warning("Daily pkl export failed or no data")
            
            # Export intraday candles to pkl
            self.logger.info("Starting intraday pkl export...")
            success_intraday, intraday_path, latest_date = data_mgr.export_intraday_candles_to_pkl(self._candle_store)
            if success_intraday and intraday_path:
                file_size = os.path.getsize(intraday_path) / (1024 * 1024)
                self.logger.info(f"Exported intraday candles for date {latest_date} to: {intraday_path} ({file_size:.2f} MB)")
                
                # Also create date-suffixed copy
                dest_intraday = os.path.join(results_dir, f"intraday_stock_data_{today_suffix}.pkl")
                if intraday_path != dest_intraday:
                    shutil.copy(intraday_path, dest_intraday)
                    self.logger.info(f"Created date-suffixed copy: intraday_stock_data_{today_suffix}.pkl")
            else:
                self.logger.warning("Intraday pkl export from candle store failed, trying ticks.json fallback...")
                
                # Fallback: convert ticks.json directly to intraday pkl
                ticks_json_path = os.path.join(results_dir, "ticks.json")
                if not os.path.exists(ticks_json_path):
                    ticks_json_path = self.json_output_path  # Use the main ticks.json path
                
                if os.path.exists(ticks_json_path):
                    self.logger.info(f"Converting ticks.json from {ticks_json_path} to intraday pkl...")
                    success_ticks, ticks_pkl_path = data_mgr.convert_ticks_json_to_pkl(ticks_json_path)
                    if success_ticks:
                        self.logger.info(f"Successfully converted ticks.json to pkl: {ticks_pkl_path}")
                    else:
                        self.logger.warning("Failed to convert ticks.json to pkl")
                else:
                    self.logger.warning(f"No ticks.json found at {ticks_json_path}")
                
        except Exception as e:
            self.logger.error(f"Error exporting pkl files: {e}")

        # Signal shutdown to all components
        self._shutdown_event.set()

        # Stop JSON writer
        if self.json_writer:
            self.json_writer.stop()

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
