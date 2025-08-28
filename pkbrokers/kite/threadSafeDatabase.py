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

import multiprocessing
import os
import queue
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from PKDevTools.classes import Archiver, log
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger

DEFAULT_PATH = Archiver.get_user_data_dir()
DEFAULT_DB_PATH = os.path.join(DEFAULT_PATH, "ticks.db")


class HighPerformanceTursoWriter:
    """Dedicated writer process for high-throughput Turso inserts"""

    def __init__(
        self,
        db_config,
        batch_size=200,
        max_queue_size=0,
        mp_context=None,
        log_level=0
        if "PKDevTools_Default_Log_Level" not in os.environ.keys()
        else int(os.environ["PKDevTools_Default_Log_Level"]),
    ):
        self.db_config = db_config
        self.batch_size = batch_size
        self.mp_context = mp_context or multiprocessing.get_context(
            "spawn" #if not sys.platform.startswith("darwin") else "fork"
        )
        self.data_queue = self.mp_context.Queue(maxsize=max_queue_size)
        self.stop_event = self.mp_context.Event()
        self.log_level = log_level
        self.setupLogger()
        self.logger = default_logger()
        self.logger.info("Starting HighPerformanceTursoWriter logger...")

    def setupLogger(self):
        if self.log_level > 0:
            os.environ["PKDevTools_Default_Log_Level"] = str(self.log_level)
        log.setup_custom_logger(
            "pkbrokers",
            self.log_level,
            trace=False,
            log_file_path="PKBrokers-log.txt",
            filter=None,
        )

    def start(self):
        """Start the writer process with proper context"""
        self.process = self.mp_context.Process(target=self._writer_loop)
        self.process.daemon = True
        self.process.start()

    def _writer_loop(self):
        """Main writer loop running in separate process"""
        try:
            import libsql
        except ImportError:
            self.logger.error("libsql package required for Turso support")
            return

        # Create connection
        conn = libsql.connect(
            database=self.db_config["turso_url"],
            auth_token=self.db_config["turso_auth_token"],
        )

        batch = []
        last_flush = time.time()
        total_inserted = 0
        consecutive_errors = 0

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # Get data from queue
                try:
                    tick_data = self.data_queue.get(timeout=0.1)
                    batch.append(tick_data)
                except queue.Empty:
                    pass

                # Flush if batch size reached or timeout
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or (current_time - last_flush) > 1.0
                )

                if should_flush and batch:
                    success = self._insert_batch(conn, batch)
                    if success:
                        total_inserted += len(batch)
                        batch = []
                        last_flush = current_time
                        consecutive_errors = 0

                        # Log every 1000 inserts
                        if total_inserted % 1000 == 0:
                            self.logger.info(f"Inserted {total_inserted} ticks total")
                    else:
                        consecutive_errors += 1
                        # Backoff on consecutive errors
                        time.sleep(min(0.1 * (2**consecutive_errors), 5.0))

            except Exception as e:
                self.logger.error(f"Writer loop error: {str(e)}")
                consecutive_errors += 1
                time.sleep(min(0.1 * (2**consecutive_errors), 5.0))

        # Final flush
        if batch:
            self._insert_batch(conn, batch)

        try:
            conn.close()
        except BaseException:
            pass

    def _insert_batch(self, conn, batch):
        """Insert a batch of ticks"""
        try:
            with conn:
                cursor = conn.cursor()

                # Prepare tick data
                tick_data = []
                depth_data = []

                for tick in batch:
                    # Convert timestamp
                    ts = (
                        tick["timestamp"].timestamp()
                        if hasattr(tick["timestamp"], "timestamp")
                        else tick["timestamp"]
                    )

                    # Tick data
                    tick_data.append(
                        (
                            tick["instrument_token"],
                            ts,
                            tick["last_price"],
                            tick["day_volume"],
                            tick["oi"],
                            tick["buy_quantity"],
                            tick["sell_quantity"],
                            tick["high_price"],
                            tick["low_price"],
                            tick["open_price"],
                            tick["prev_day_close"],
                        )
                    )

                    # Depth data
                    if "depth" in tick and tick["depth"]:
                        inst = tick["instrument_token"]

                        # Process bids
                        for i, bid in enumerate(tick["depth"].get("bid", [])[:5], 1):
                            depth_data.append(
                                (
                                    inst,
                                    ts,
                                    "bid",
                                    i,
                                    bid.get("price", 0),
                                    bid.get("quantity", 0),
                                    bid.get("orders", 0),
                                )
                            )

                        # Process asks
                        for i, ask in enumerate(tick["depth"].get("ask", [])[:5], 1):
                            depth_data.append(
                                (
                                    inst,
                                    ts,
                                    "ask",
                                    i,
                                    ask.get("price", 0),
                                    ask.get("quantity", 0),
                                    ask.get("orders", 0),
                                )
                            )

                # Batch insert ticks
                if tick_data:
                    cursor.executemany(TICK_INSERT_SQL, tick_data)

                # Batch insert depth
                if depth_data:
                    cursor.executemany(DEPTH_INSERT_SQL, depth_data)

                return True

        except Exception as e:
            self.logger.error(f"Batch insert failed: {str(e)}")
            return False

    def add_ticks(self, ticks):
        """Add ticks to the write queue"""
        for tick in ticks:
            try:
                self.data_queue.put(tick, timeout=0.1)
            except queue.Full:
                self.logger.warn("Write queue full, dropping ticks")
                break

    def stop(self):
        """Stop the writer"""
        self.stop_event.set()
        try:
            self.process.join(timeout=5)
        except BaseException:
            pass


# SQL templates for batch inserts
TICK_INSERT_SQL = """
INSERT INTO ticks (
    instrument_token, timestamp, last_price, day_volume, oi,
    buy_quantity, sell_quantity, high_price, low_price,
    open_price, prev_day_close
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

DEPTH_INSERT_SQL = """
INSERT INTO market_depth (
    instrument_token, timestamp, depth_type,
    position, price, quantity, orders
) VALUES (?, ?, ?, ?, ?, ?, ?)
"""


class ThreadSafeDatabase:
    def __init__(
        self,
        db_type: str = PKEnvironment().DB_TYPE,  # "local" or "turso"
        db_path: Optional[str] = None,
        turso_url: Optional[str] = PKEnvironment().TDU,
        turso_auth_token: Optional[str] = PKEnvironment().TAT,
        max_batch_size: int = 200,
        num_writers: int = 3,  # Multiple writers for Turso
        mp_context=None,  # Explicit multiprocessing context
        log_level=0
        if "PKDevTools_Default_Log_Level" not in os.environ.keys()
        else int(os.environ["PKDevTools_Default_Log_Level"]),
    ):
        self.db_type = db_type.lower()
        self.db_path = db_path or os.path.join(DEFAULT_PATH, "ticks.db")
        self.turso_url = turso_url
        self.turso_auth_token = turso_auth_token
        self.max_batch_size = max_batch_size
        self.num_writers = num_writers if db_type == "turso" else 1
        self.log_level = log_level
        # Use consistent multiprocessing context
        self.mp_context = mp_context or multiprocessing.get_context(
            "spawn" #if not sys.platform.startswith("darwin") else "fork"
        )

        self.local = threading.local()
        self.lock = threading.Lock()
        # Initialize process-specific logger
        self.setupLogger()
        self.logger = default_logger()
        self.logger.info("Starting ThreadSafeDatabase logger...")

        # For Turso: use dedicated writer processes
        self.turso_writers = []
        self.write_queue = queue.Queue(maxsize=0)

        # Initialize the appropriate database
        self._initialize_db()

        # Start Turso writers if needed
        if self.db_type == "turso":
            self._start_turso_writers()

    def _start_turso_writers(self):
        """Start multiple writer processes for Turso with proper context"""
        for i in range(self.num_writers):
            writer = HighPerformanceTursoWriter(
                db_config={
                    "turso_url": self.turso_url,
                    "turso_auth_token": self.turso_auth_token,
                },
                batch_size=self.max_batch_size,
                mp_context=self.mp_context,  # Pass the same context
                log_level=self.log_level
            )
            writer.start()
            self.turso_writers.append(writer)
            self.logger.info(f"Started Turso writer process {i + 1}")

    def _initialize_db(self, force_drop: bool = False):
        """Initialize database schema - optimized for batch inserts"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if force_drop and self.db_type == "local":
                cursor.execute("DROP TABLE IF EXISTS market_depth")
                cursor.execute("DROP TABLE IF EXISTS ticks")

            # Main ticks table - optimized structure
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_token INTEGER,
                    timestamp INTEGER,
                    last_price REAL,
                    day_volume INTEGER,
                    oi INTEGER,
                    buy_quantity INTEGER,
                    sell_quantity INTEGER,
                    high_price REAL,
                    low_price REAL,
                    open_price REAL,
                    prev_day_close REAL,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                ) STRICT
            """)

            # Market depth table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_depth (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_token INTEGER,
                    timestamp INTEGER,
                    depth_type TEXT CHECK(depth_type IN ('bid', 'ask')),
                    position INTEGER CHECK(position BETWEEN 1 AND 5),
                    price REAL,
                    quantity INTEGER,
                    orders INTEGER,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                ) STRICT
            """)

            # Optimized indexes for batch inserts
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticks_main
                ON ticks(instrument_token, timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_depth_main
                ON market_depth(instrument_token, timestamp, depth_type)
            """)

            # Local database optimizations
            if self.db_type == "local":
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous = NORMAL")
                cursor.execute("PRAGMA cache_size = -100000")  # 100MB cache
                cursor.execute("PRAGMA temp_store = MEMORY")
                cursor.execute("PRAGMA mmap_size = 30000000000")  # 30GB mmap

            conn.commit()

    def _get_local_connection(self):
        """Get optimized local SQLite connection"""
        if not hasattr(self.local, "conn"):
            self.local.conn = sqlite3.connect(self.db_path, timeout=30)
            # Optimize connection for batch inserts
            self.local.conn.execute("PRAGMA journal_mode=WAL")
            self.local.conn.execute("PRAGMA synchronous = NORMAL")
            self.local.conn.execute("PRAGMA cache_size = -100000")
            self.local.conn.execute("PRAGMA temp_store = MEMORY")
        return self.local.conn

    def _get_turso_connection(self, force_connect=False):
        """Get Turso connection - only for queries, not for inserts"""
        try:
            import libsql

            if not hasattr(self.local, "conn") or force_connect:
                self.local.conn = libsql.connect(
                    database=self.turso_url, auth_token=self.turso_auth_token
                )
            return self.local.conn
        except ImportError:
            raise ImportError("libsql package required for Turso support")

    def setupLogger(self):
        if self.log_level > 0:
            os.environ["PKDevTools_Default_Log_Level"] = str(self.log_level)
        log.setup_custom_logger(
            "pkbrokers",
            self.log_level,
            trace=False,
            log_file_path="PKBrokers-log.txt",
            filter=None,
        )

    @contextmanager
    def get_connection(self, force_connect=False):
        """Get connection for queries"""
        if self.db_type == "local":
            conn = self._get_local_connection()
        elif self.db_type == "turso":
            conn = self._get_turso_connection(force_connect=force_connect)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

        try:
            yield conn
        except Exception as e:
            try:
                conn.rollback()
            except BaseException:
                pass
            raise e

    def insert_ticks(
        self, ticks: List[Dict[str, Any]], force_connect=False, retrial=False
    ):
        """High-performance batch insert"""
        if not ticks:
            return

        # For Turso: use dedicated writers
        if self.db_type == "turso":
            for writer in self.turso_writers:
                writer.add_ticks(ticks)
            return

        # For local: optimized batch insert
        with self.lock, self.get_connection(force_connect=force_connect) as conn:
            try:
                cursor = conn.cursor()

                # Prepare data in bulk
                tick_data = []
                depth_data = []

                for tick in ticks:
                    # Convert timestamp
                    ts = (
                        tick["timestamp"].timestamp()
                        if hasattr(tick["timestamp"], "timestamp")
                        else tick["timestamp"]
                    )

                    # Tick data
                    tick_data.append(
                        (
                            tick["instrument_token"],
                            ts,
                            tick["last_price"],
                            tick["day_volume"],
                            tick["oi"],
                            tick["buy_quantity"],
                            tick["sell_quantity"],
                            tick["high_price"],
                            tick["low_price"],
                            tick["open_price"],
                            tick["prev_day_close"],
                        )
                    )

                    # Depth data
                    if "depth" in tick and tick["depth"]:
                        inst = tick["instrument_token"]

                        # Process bids
                        for i, bid in enumerate(tick["depth"].get("bid", [])[:5], 1):
                            depth_data.append(
                                (
                                    inst,
                                    ts,
                                    "bid",
                                    i,
                                    bid.get("price", 0),
                                    bid.get("quantity", 0),
                                    bid.get("orders", 0),
                                )
                            )

                        # Process asks
                        for i, ask in enumerate(tick["depth"].get("ask", [])[:5], 1):
                            depth_data.append(
                                (
                                    inst,
                                    ts,
                                    "ask",
                                    i,
                                    ask.get("price", 0),
                                    ask.get("quantity", 0),
                                    ask.get("orders", 0),
                                )
                            )

                # Batch insert ticks
                if tick_data:
                    cursor.executemany(TICK_INSERT_SQL, tick_data)

                # Batch insert depth
                if depth_data:
                    cursor.executemany(DEPTH_INSERT_SQL, depth_data)

                conn.commit()

                # Log performance
                if len(ticks) > 50:
                    self.logger.debug(f"Inserted {len(ticks)} ticks in batch")

            except Exception as e:
                self.logger.error(f"Batch insert error: {str(e)}")
                try:
                    conn.rollback()
                except BaseException:
                    pass

                # Handle reconnection
                if "operational" in str(e).lower() and not retrial:
                    self.close_all()
                    self.insert_ticks(ticks=ticks, force_connect=True, retrial=True)

    def close_all(self):
        """Close all connections and writers"""
        # Close thread connections
        if hasattr(self.local, "conn"):
            try:
                self.local.conn.close()
                delattr(self.local, "conn")
            except BaseException:
                pass

        # Stop Turso writers
        for writer in self.turso_writers:
            try:
                writer.stop()
            except BaseException:
                pass
        self.turso_writers = []

    def get_ohlcv(
        self,
        instrument_token: int,
        timeframe_minutes: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get OHLCV data for a specific instrument and timeframe

        Args:
            instrument_token: Instrument token
            timeframe_minutes: Timeframe in minutes (1, 5, 10, 15, 30, 60)
            start_time: Unix timestamp for start time (optional)
            end_time: Unix timestamp for end time (optional)
            limit: Maximum number of candles to return

        Returns:
            List of OHLCV candles with timestamp, open, high, low, close, volume
        """
        if timeframe_minutes not in [1, 5, 10, 15, 30, 60]:
            raise ValueError("Timeframe must be 1, 5, 10, 15, 30, or 60 minutes")

        timeframe_seconds = timeframe_minutes * 60

        # Build WHERE clause
        where_clause = "WHERE instrument_token = ?"
        params = [instrument_token]

        if start_time:
            where_clause += " AND timestamp >= ?"
            params.append(start_time)

        if end_time:
            where_clause += " AND timestamp <= ?"
            params.append(end_time)

        sql = f"""
            SELECT
                (timestamp / {timeframe_seconds}) * {timeframe_seconds} as candle_time,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp,
                FIRST_VALUE(last_price) OVER (
                    PARTITION BY (timestamp / {timeframe_seconds})
                    ORDER BY timestamp
                ) as open_price,
                MAX(last_price) as high_price,
                MIN(last_price) as low_price,
                LAST_VALUE(last_price) OVER (
                    PARTITION BY (timestamp / {timeframe_seconds})
                    ORDER BY timestamp
                ) as close_price,
                SUM(day_volume) as total_volume,
                COUNT(*) as tick_count,
                AVG(oi) as avg_oi,
                SUM(buy_quantity) as total_buy_quantity,
                SUM(sell_quantity) as total_sell_quantity
            FROM ticks
            {where_clause}
            GROUP BY candle_time
            ORDER BY candle_time DESC
            LIMIT ?
        """

        params.append(limit)

        results = self.query(sql, tuple(params))

        ohlcv_data = []
        for row in results:
            ohlcv_data.append(
                {
                    "instrument_token": instrument_token,
                    "timestamp": row[0],
                    "start_time": row[1],
                    "end_time": row[2],
                    "open": row[3],
                    "high": row[4],
                    "low": row[5],
                    "close": row[6],
                    "volume": row[7],
                    "tick_count": row[8],
                    "oi": row[9],
                    "buy_quantity": row[10],
                    "sell_quantity": row[11],
                    "timeframe": f"{timeframe_minutes}min",
                }
            )

        return ohlcv_data

    def get_ohlcv_multiple(
        self,
        instrument_tokens: List[int],
        timeframe_minutes: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit_per_instrument: int = 1000,
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Get OHLCV data for multiple instruments and timeframe

        Args:
            instrument_tokens: List of instrument tokens
            timeframe_minutes: Timeframe in minutes (1, 5, 10, 15, 30, 60)
            start_time: Unix timestamp for start time (optional)
            end_time: Unix timestamp for end time (optional)
            limit_per_instrument: Maximum candles per instrument

        Returns:
            Dictionary with instrument_token as key and list of OHLCV candles as value
        """
        if not instrument_tokens:
            return {}

        results = {}
        for instrument_token in instrument_tokens:
            ohlcv_data = self.get_ohlcv(
                instrument_token,
                timeframe_minutes,
                start_time,
                end_time,
                limit_per_instrument,
            )
            results[instrument_token] = ohlcv_data

        return results

    def get_ohlcv_range(
        self,
        instrument_token: int,
        timeframe_minutes: int,
        start_time: int,
        end_time: int,
    ) -> List[Dict[str, Any]]:
        """
        Get OHLCV data for a specific instrument and timeframe within a time range
        """
        return self.get_ohlcv(
            instrument_token, timeframe_minutes, start_time, end_time, 10000
        )

    # Convenience methods for common timeframes
    def get_1min_ohlcv(self, instrument_token: int, **kwargs) -> List[Dict[str, Any]]:
        """Get 1-minute OHLCV data"""
        return self.get_ohlcv(instrument_token, 1, **kwargs)

    def get_5min_ohlcv(self, instrument_token: int, **kwargs) -> List[Dict[str, Any]]:
        """Get 5-minute OHLCV data"""
        return self.get_ohlcv(instrument_token, 5, **kwargs)

    def get_10min_ohlcv(self, instrument_token: int, **kwargs) -> List[Dict[str, Any]]:
        """Get 10-minute OHLCV data"""
        return self.get_ohlcv(instrument_token, 10, **kwargs)

    def get_15min_ohlcv(self, instrument_token: int, **kwargs) -> List[Dict[str, Any]]:
        """Get 15-minute OHLCV data"""
        return self.get_ohlcv(instrument_token, 15, **kwargs)

    def get_30min_ohlcv(self, instrument_token: int, **kwargs) -> List[Dict[str, Any]]:
        """Get 30-minute OHLCV data"""
        return self.get_ohlcv(instrument_token, 30, **kwargs)

    def get_1hour_ohlcv(self, instrument_token: int, **kwargs) -> List[Dict[str, Any]]:
        """Get 1-hour OHLCV data"""
        return self.get_ohlcv(instrument_token, 60, **kwargs)

    def get_day_ohlcv(
        self, instrument_token: int, reference_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get OHLCV data for the current trading day from market start until now

        Args:
            instrument_token: Instrument token
            reference_time: Optional reference time (defaults to current time)

        Returns:
            Dictionary with OHLCV data for the trading day
        """
        if reference_time is None:
            reference_time = datetime.now()

        # Get market start time for the day (9:15 AM IST)
        market_start_time = reference_time.replace(
            hour=9, minute=15, second=0, microsecond=0
        )

        # If current time is before market open, use previous day
        if reference_time.time() < market_start_time.time():
            market_start_time = market_start_time - timedelta(days=1)

        # Convert to Unix timestamps
        market_start_timestamp = int(market_start_time.timestamp())
        current_timestamp = int(reference_time.timestamp())

        # Get all ticks for the current trading day
        sql = """
            SELECT
                timestamp,
                last_price,
                day_volume,
                high_price,
                low_price,
                open_price
            FROM ticks
            WHERE instrument_token = ? AND timestamp >= ?
            ORDER BY timestamp
        """

        results = self.query(sql, (instrument_token, market_start_timestamp))

        if not results:
            return {
                "instrument_token": instrument_token,
                "date": reference_time.date().isoformat(),
                "open": 0,
                "high": 0,
                "low": 0,
                "close": 0,
                "volume": 0,
                "tick_count": 0,
                "market_start_time": market_start_timestamp,
                "current_time": current_timestamp,
                "data_available": False,
            }

        # Extract data
        timestamps, prices, volumes, highs, lows, opens = zip(*results)

        # Calculate OHLCV
        open_price = opens[0] if opens else 0
        high_price = max(highs) if highs else 0
        low_price = min(lows) if lows else 0
        close_price = prices[-1] if prices else 0
        total_volume = sum(volumes) if volumes else 0

        return {
            "instrument_token": instrument_token,
            "date": reference_time.date().isoformat(),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": total_volume,
            "tick_count": len(results),
            "market_start_time": market_start_timestamp,
            "current_time": current_timestamp,
            "data_available": True,
            "first_tick_time": timestamps[0] if timestamps else None,
            "last_tick_time": timestamps[-1] if timestamps else None,
        }

    def get_day_ohlcv_multiple(
        self, instrument_tokens: List[int], reference_time: Optional[datetime] = None
    ) -> Dict[int, Dict[str, Any]]:
        """
        Get OHLCV data for multiple instruments for the current trading day

        Args:
            instrument_tokens: List of instrument tokens
            reference_time: Optional reference time (defaults to current time)

        Returns:
            Dictionary with instrument_token as key and OHLCV data as value
        """
        results = {}
        for token in instrument_tokens:
            results[token] = self.get_day_ohlcv(token, reference_time)
        return results

    def get_intraday_ohlcv(
        self,
        instrument_token: int,
        interval_minutes: int = 5,
        reference_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get intraday OHLCV data for the current trading day with specified interval

        Args:
            instrument_token: Instrument token
            interval_minutes: Time interval in minutes (1, 5, 15, 30, 60)
            reference_time: Optional reference time (defaults to current time)

        Returns:
            List of OHLCV candles for the trading day
        """
        if reference_time is None:
            reference_time = datetime.now()

        # Get market start time for the day (9:15 AM IST)
        market_start_time = reference_time.replace(
            hour=9, minute=15, second=0, microsecond=0
        )

        # If current time is before market open, use previous day
        if reference_time.time() < market_start_time.time():
            market_start_time = market_start_time - timedelta(days=1)

        market_start_timestamp = int(market_start_time.timestamp())
        current_timestamp = int(reference_time.timestamp())

        return self.get_ohlcv_range(
            instrument_token,
            interval_minutes,
            market_start_timestamp,
            current_timestamp,
        )

    def get_today_high_low(self, instrument_token: int) -> Dict[str, float]:
        """
        Get today's high and low prices for an instrument
        """
        ohlcv = self.get_day_ohlcv(instrument_token)
        return {
            "instrument_token": instrument_token,
            "high": ohlcv["high"],
            "low": ohlcv["low"],
            "current": ohlcv["close"],
        }

    def get_daily_performance(self, instrument_token: int) -> Dict[str, Any]:
        """
        Get daily performance statistics including percentage change
        """
        ohlcv = self.get_day_ohlcv(instrument_token)

        if not ohlcv["data_available"] or ohlcv["open"] == 0:
            return {
                "instrument_token": instrument_token,
                "change": 0,
                "change_percent": 0,
                "high": 0,
                "low": 0,
                "volume": 0,
                "performance": "no_data",
            }

        change = ohlcv["close"] - ohlcv["open"]
        change_percent = (change / ohlcv["open"]) * 100 if ohlcv["open"] != 0 else 0

        # Determine performance category
        if change_percent > 2:
            performance = "very_bullish"
        elif change_percent > 0.5:
            performance = "bullish"
        elif change_percent > -0.5:
            performance = "neutral"
        elif change_percent > -2:
            performance = "bearish"
        else:
            performance = "very_bearish"

        return {
            "instrument_token": instrument_token,
            "open": ohlcv["open"],
            "high": ohlcv["high"],
            "low": ohlcv["low"],
            "close": ohlcv["close"],
            "volume": ohlcv["volume"],
            "change": change,
            "change_percent": change_percent,
            "performance": performance,
            "tick_count": ohlcv["tick_count"],
        }

    def get_market_summary(self, instrument_tokens: List[int]) -> Dict[str, Any]:
        """
        Get market summary for multiple instruments
        """
        summary = {
            "total_instruments": len(instrument_tokens),
            "bullish": 0,
            "bearish": 0,
            "neutral": 0,
            "total_volume": 0,
            "average_change": 0,
            "top_gainers": [],
            "top_losers": [],
            "most_active": [],
        }

        performances = []
        for token in instrument_tokens:
            performance = self.get_daily_performance(token)
            performances.append(performance)

            summary["total_volume"] += performance["volume"]

            if (
                performance["performance"] == "bullish"
                or performance["performance"] == "very_bullish"
            ):
                summary["bullish"] += 1
            elif (
                performance["performance"] == "bearish"
                or performance["performance"] == "very_bearish"
            ):
                summary["bearish"] += 1
            else:
                summary["neutral"] += 1

        # Calculate average change
        if performances:
            valid_changes = [
                p["change_percent"] for p in performances if p["change_percent"] != 0
            ]
            if valid_changes:
                summary["average_change"] = sum(valid_changes) / len(valid_changes)

        # Sort for top gainers/losers
        performances.sort(key=lambda x: x["change_percent"], reverse=True)
        summary["top_gainers"] = performances[:5]
        summary["top_losers"] = (
            performances[-5:] if len(performances) >= 5 else performances
        )

        # Sort for most active by volume
        performances.sort(key=lambda x: x["volume"], reverse=True)
        summary["most_active"] = performances[:5]

        return summary

    def get_latest_ohlcv(
        self, instrument_token: int, timeframe_minutes: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get the latest OHLCV candle for a specific instrument and timeframe
        """
        ohlcv_data = self.get_ohlcv(instrument_token, timeframe_minutes, limit=1)
        return ohlcv_data[0] if ohlcv_data else None

    def get_ohlcv_with_depth(
        self,
        instrument_token: int,
        timeframe_minutes: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get OHLCV data with market depth information for each candle
        """
        ohlcv_data = self.get_ohlcv(
            instrument_token, timeframe_minutes, start_time, end_time, limit
        )

        for candle in ohlcv_data:
            # Get market depth at the end of the candle period
            depth = self.get_market_depth(instrument_token, candle["end_time"])
            candle["market_depth"] = depth

        return ohlcv_data

    def get_volume_profile(
        self,
        instrument_token: int,
        timeframe_minutes: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get volume profile analysis for a specific instrument and timeframe
        """
        ohlcv_data = self.get_ohlcv(
            instrument_token, timeframe_minutes, start_time, end_time, 10000
        )

        if not ohlcv_data:
            return {}

        # Calculate volume profile statistics
        total_volume = sum(candle["volume"] for candle in ohlcv_data)
        avg_volume = total_volume / len(ohlcv_data) if ohlcv_data else 0

        # Find high volume periods
        high_volume_candles = [
            candle for candle in ohlcv_data if candle["volume"] > avg_volume * 1.5
        ]

        return {
            "instrument_token": instrument_token,
            "timeframe": f"{timeframe_minutes}min",
            "total_volume": total_volume,
            "average_volume": avg_volume,
            "volume_candles_count": len(ohlcv_data),
            "high_volume_periods": len(high_volume_candles),
            "high_volume_ratio": len(high_volume_candles) / len(ohlcv_data)
            if ohlcv_data
            else 0,
            "period_start": ohlcv_data[-1]["timestamp"] if ohlcv_data else None,
            "period_end": ohlcv_data[0]["timestamp"] if ohlcv_data else None,
        }

    def query(self, sql: str, params: tuple = ()) -> List[tuple]:
        """Execute a query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()

    def execute(self, sql: str, params: tuple = ()) -> None:
        """Execute a SQL statement without returning results"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()

    def batch_execute(self, sql: str, params_list: List[tuple]) -> None:
        """Execute multiple SQL statements with different parameters"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, params_list)
            conn.commit()

    def get_instrument_data(
        self, instrument_token: int, limit: int = 100
    ) -> List[dict]:
        """Get tick data for a specific instrument (multiple entries)"""
        sql = "SELECT * FROM ticks WHERE instrument_token = ? ORDER BY timestamp DESC LIMIT ?"
        results = self.query(sql, (instrument_token, limit))

        if results:
            columns = [col[0] for col in self.get_connection().cursor().description]
            return [dict(zip(columns, row)) for row in results]
        return []

    def get_latest_instrument_data(self, instrument_token: int) -> Optional[dict]:
        """Get the latest tick data for a specific instrument"""
        sql = "SELECT * FROM ticks WHERE instrument_token = ? ORDER BY timestamp DESC LIMIT 1"
        result = self.query(sql, (instrument_token,))

        if result:
            columns = [col[0] for col in self.get_connection().cursor().description]
            return dict(zip(columns, result[0]))
        return None

    def get_market_depth(
        self, instrument_token: int, timestamp: Optional[int] = None
    ) -> Dict[str, list]:
        """Get market depth for a specific instrument at a specific timestamp"""
        depth = {"bid": [], "ask": []}

        if timestamp:
            # Get depth for specific timestamp
            bid_sql = """
                SELECT position, price, quantity, orders
                FROM market_depth
                WHERE instrument_token = ? AND depth_type = 'bid' AND timestamp = ?
                ORDER BY position
            """
            ask_sql = """
                SELECT position, price, quantity, orders
                FROM market_depth
                WHERE instrument_token = ? AND depth_type = 'ask' AND timestamp = ?
                ORDER BY position
            """
            bid_params = (instrument_token, timestamp)
            ask_params = (instrument_token, timestamp)
        else:
            # Get latest depth
            bid_sql = """
                SELECT position, price, quantity, orders
                FROM market_depth
                WHERE instrument_token = ? AND depth_type = 'bid'
                ORDER BY timestamp DESC, position
                LIMIT 5
            """
            ask_sql = """
                SELECT position, price, quantity, orders
                FROM market_depth
                WHERE instrument_token = ? AND depth_type = 'ask'
                ORDER BY timestamp DESC, position
                LIMIT 5
            """
            bid_params = (instrument_token,)
            ask_params = (instrument_token,)

        # Get bids
        bids = self.query(bid_sql, bid_params)
        for position, price, quantity, orders in bids:
            depth["bid"].append(
                {
                    "position": position,
                    "price": price,
                    "quantity": quantity,
                    "orders": orders,
                }
            )

        # Get asks
        asks = self.query(ask_sql, ask_params)
        for position, price, quantity, orders in asks:
            depth["ask"].append(
                {
                    "position": position,
                    "price": price,
                    "quantity": quantity,
                    "orders": orders,
                }
            )

        return depth
