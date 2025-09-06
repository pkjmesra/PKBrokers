import json
import os
import sqlite3
import time
from datetime import datetime
from queue import Empty

import libsql
import pytz
from PKDevTools.classes import log
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities

from pkbrokers.kite.threadSafeDatabase import DEFAULT_DB_PATH
from pkbrokers.kite.ticks import Tick

OPTIMAL_BATCH_SIZE = 100  # Adjust based on testing


class DatabaseWriterProcess:
    """
    Separate process for handling database writes with support for both local SQLite and Turso
    using libSQL for both cases
    """

    def __init__(self, data_queue, stop_event, log_level=None):
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.db_config = {
            "type": PKEnvironment().DB_TYPE,
            "url": PKEnvironment().TDU,
            "auth_token": PKEnvironment().TAT,
        }
        self.log_level = log_level
        self.logger = None
        self.batch_size = OPTIMAL_BATCH_SIZE
        self.max_batch_wait = 1.0  # seconds
        self.db_type = self.db_config.get("type", "local")  # 'local' or 'turso'
        self.conn = None

    def setup_logger(self):
        """Setup process-specific logger"""
        if self.log_level is not None:
            os.environ["PKDevTools_Default_Log_Level"] = str(self.log_level)
        log.setup_custom_logger(
            "pkbrokersDB",
            self.log_level,
            trace=False,
            log_file_path="PKBrokers-DBlog.txt",
            filter=None,
        )
        self.logger = default_logger()

    def _create_connection(self):
        """Create appropriate database connection based on type using libSQL"""
        if self.db_type == "turso":
            return self._create_turso_connection()
        else:
            return self._create_local_connection()

    def _create_local_connection(self):
        """Create local SQLite connection using libSQL"""
        db_path = self.db_config.get("path", DEFAULT_DB_PATH)
        try:
            if libsql:
                conn = libsql.connect(db_path)
            else:
                conn = sqlite3.connect(db_path, check_same_thread=False)

            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size = -100000")  # 100MB cache
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 30000000000")  # 30GB mmap
            return conn
        except Exception as e:
            self.logger.error(f"Failed to create local connection: {str(e)}")
            raise

    def _create_turso_connection(self):
        """Create connection to Turso database using libSQL"""
        try:
            if not libsql:
                raise ImportError(
                    "libsql_experimental package is required for Turso support"
                )

            url = self.db_config.get("url")
            auth_token = self.db_config.get("auth_token")

            if not url or not auth_token:
                raise ValueError("Turso configuration requires both URL and auth token")

            # Create libSQL connection to Turso
            conn = libsql.connect(database=url, auth_token=auth_token)

            # Set appropriate pragmas for remote database
            # conn.execute("PRAGMA synchronous=NORMAL")
            return conn

        except Exception as e:
            self.logger.error(f"Failed to create Turso connection: {str(e)}")
            raise

    def _insert_batch_turso(self, conn, ticks, force_connect=False, retrial=False):
        """Thread-safe batch insert with market depth for both local and turso"""
        if not ticks:
            return conn

        with conn:
            try:
                cursor = conn.cursor()

                # Prepare tick data - convert to proper numeric timestamp (Unix timestamp)
                tick_data = [
                    (
                        t["instrument_token"],
                        t["timestamp"].timestamp()
                        if hasattr(t["timestamp"], "timestamp")
                        else t["timestamp"],
                        t["last_price"],
                        t["day_volume"],
                        t["oi"],
                        t["buy_quantity"],
                        t["sell_quantity"],
                        t["high_price"],
                        t["low_price"],
                        t["open_price"],
                        t["prev_day_close"],
                    )
                    for t in ticks
                ]

                # Simple INSERT for ticks (no UPSERT)
                insert_sql = """
                    INSERT INTO ticks (
                        instrument_token, timestamp, last_price, day_volume, oi,
                        buy_quantity, sell_quantity, high_price, low_price,
                        open_price, prev_day_close
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                cursor.executemany(insert_sql, tick_data)

                # Insert market depth data
                depth_data = []
                for tick in ticks:
                    if "depth" in tick and tick["depth"]:
                        # Convert timestamp to numeric (Unix timestamp)
                        ts = (
                            tick["timestamp"].timestamp()
                            if hasattr(tick["timestamp"], "timestamp")
                            else tick["timestamp"]
                        )
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

                if depth_data:
                    # Simple INSERT for market depth (no UPSERT)
                    depth_insert_sql = """
                        INSERT INTO market_depth (
                            instrument_token, timestamp, depth_type,
                            position, price, quantity, orders
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """

                    cursor.executemany(depth_insert_sql, depth_data)

                conn.commit()
                self.logger.info(
                    f"Inserted {len(ticks)} ticks to {self.db_type} database."
                )

            except Exception as e:
                self.logger.error(f"Database insert error: {str(e)}")
                try:
                    if hasattr(conn, "rollback"):
                        conn.rollback()
                except BaseException:
                    pass
                if (
                    self.db_type == "turso"
                    and "stream not found" in str(e).lower()
                    and not retrial
                ) or ("connection" in str(e).lower() or "network" in str(e).lower()):
                    self.logger.error(
                        "Reinitializing turso database connection due to stream error"
                    )
                    try:
                        conn.close()
                        conn = self._create_turso_connection()
                    except BaseException:
                        conn = self._create_turso_connection()
                    self._insert_batch_turso(
                        ticks=ticks, force_connect=True, retrial=True
                    )

        return conn

    def _insert_batch_local(self, conn, batch):
        """Insert batch into local SQLite database using libSQL"""
        if not batch:
            return conn

        try:
            with conn:
                placeholders = ", ".join(["?"] * 12)
                sql = f"""
                INSERT OR IGNORE INTO ticks
                (instrument_token, timestamp, last_price, day_volume, oi,
                 buy_quantity, sell_quantity, high_price, low_price,
                 open_price, prev_day_close, depth)
                VALUES ({placeholders})
                """

                data = []
                for tick in batch:
                    depth_json = (
                        json.dumps(tick.get("depth", {})) if tick.get("depth") else None
                    )
                    timestamp_str = (
                        tick["timestamp"].isoformat()
                        if isinstance(tick["timestamp"], datetime)
                        else tick["timestamp"]
                    )

                    data.append(
                        (
                            tick["instrument_token"],
                            timestamp_str,
                            tick["last_price"],
                            tick["day_volume"],
                            tick["oi"],
                            tick["buy_quantity"],
                            tick["sell_quantity"],
                            tick["high_price"],
                            tick["low_price"],
                            tick["open_price"],
                            tick["prev_day_close"],
                            depth_json,
                        )
                    )

                conn.executemany(sql, data)

        except Exception as e:
            self.logger.error(f"Local batch insert error: {str(e)}")
            conn.close()
            return self._create_local_connection()

        return conn

    def _insert_batch(self, conn, batch):
        """Insert batch using appropriate method based on database type"""
        if self.db_type == "turso":
            return self._insert_batch_turso(conn, batch)
        else:
            return self._insert_batch_local(conn, batch)

    def run(self):
        """Main database writer process"""
        self.setup_logger()
        self.logger.info(
            f"Starting database writer process for {self.db_type} database"
        )

        connection = self._create_connection()
        batch = []
        last_flush_time = time.time()
        insert_count = 0
        last_stats_time = time.time()
        consecutive_errors = 0
        max_consecutive_errors = 10

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # Get data from queue with timeout
                try:
                    tick_data = self.data_queue.get(timeout=0.1)
                    if tick_data and tick_data.get("type") == "tick":
                        processed = self._process_tick_data(tick_data)
                        batch.append(processed)
                        insert_count += 1
                        consecutive_errors = (
                            0  # Reset error counter on successful processing
                        )
                except Empty:
                    pass

                # Check if we should flush the batch
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size
                    or (current_time - last_flush_time) >= self.max_batch_wait
                )

                if should_flush and batch:
                    connection = self._insert_batch(connection, batch)
                    batch = []
                    last_flush_time = current_time

                # Log statistics periodically
                if current_time - last_stats_time > 30:
                    queue_size = self.data_queue.qsize()
                    self.logger.info(
                        f"Database writer processed {insert_count} ticks, "
                        f"queue size: {queue_size}, "
                        f"batch size: {len(batch)}"
                    )
                    insert_count = 0
                    last_stats_time = current_time

            except Exception as e:
                consecutive_errors += 1
                self.logger.error(
                    f"Database writer error ({consecutive_errors}): {str(e)}"
                )

                # If we have too many consecutive errors, sleep longer to avoid tight loop
                sleep_time = min(0.1 * (2**consecutive_errors), 5.0)
                time.sleep(sleep_time)

                # Recreate connection if we have persistent errors
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.warn(
                        "Recreating database connection due to persistent errors"
                    )
                    try:
                        if hasattr(connection, "close"):
                            connection.close()
                    except BaseException:
                        pass
                    connection = self._create_connection()
                    consecutive_errors = 0

        # Flush any remaining data
        if batch:
            try:
                self._insert_batch(connection, batch)
            except Exception as e:
                self.logger.error(f"Final flush error: {str(e)}")

        # Close connection
        try:
            if hasattr(connection, "close"):
                connection.close()
        except BaseException:
            pass

        self.logger.info("Database writer process stopped")

    def _process_tick_data(self, tick_data):
        """Process tick data for database insertion"""
        if tick_data["exchange_timestamp"] is None:
            tick_data["exchange_timestamp"] = PKDateUtilities.currentDateTimestamp()
        tick = self._convert_tick_data_to_object(tick_data)

        if tick.exchange_timestamp is None:
            tick.exchange_timestamp = PKDateUtilities.currentDateTimestamp()

        processed = {
            "instrument_token": tick.instrument_token,
            "timestamp": datetime.fromtimestamp(
                tick.exchange_timestamp, tz=pytz.timezone("Asia/Kolkata")
            ),
            "last_price": tick.last_price if tick.last_price is not None else 0,
            "day_volume": tick.day_volume if tick.day_volume is not None else 0,
            "oi": tick.oi if tick.oi is not None else 0,
            "buy_quantity": tick.buy_quantity if tick.buy_quantity is not None else 0,
            "sell_quantity": tick.sell_quantity
            if tick.sell_quantity is not None
            else 0,
            "high_price": tick.high_price if tick.high_price is not None else 0,
            "low_price": tick.low_price if tick.low_price is not None else 0,
            "open_price": tick.open_price if tick.open_price is not None else 0,
            "prev_day_close": tick.prev_day_close
            if tick.prev_day_close is not None
            else 0,
        }

        if tick.depth:
            processed["depth"] = {
                "bid": [
                    {
                        "price": b.get("price", 0)
                        if isinstance(b, dict)
                        else (b.price if b.price is not None else 0),
                        "quantity": b.get("quantity", 0)
                        if isinstance(b, dict)
                        else (b.quantity if b.quantity is not None else 0),
                        "orders": b.get("orders", 0)
                        if isinstance(b, dict)
                        else (b.orders if b.orders is not None else 0),
                    }
                    for b in tick.depth.get("bid", [])[:5]
                ],
                "ask": [
                    {
                        "price": a.get("price", 0)
                        if isinstance(a, dict)
                        else (a.price if a.price is not None else 0),
                        "quantity": a.get("quantity", 0)
                        if isinstance(a, dict)
                        else (a.quantity if a.quantity is not None else 0),
                        "orders": a.get("orders", 0)
                        if isinstance(a, dict)
                        else (a.orders if a.orders is not None else 0),
                    }
                    for a in tick.depth.get("ask", [])[:5]
                ],
            }

        return processed

    def _convert_tick_data_to_object(self, tick_data):
        """Convert tick data dictionary back to Tick object."""
        tick = Tick(
            instrument_token=tick_data.get("instrument_token", 0),
            last_price=tick_data.get("last_price", 0),
            last_quantity=tick_data.get("last_quantity", 0),
            avg_price=tick_data.get("avg_price", 0),
            day_volume=tick_data.get("day_volume", 0),
            buy_quantity=tick_data.get("buy_quantity", 0),
            sell_quantity=tick_data.get("sell_quantity", 0),
            open_price=tick_data.get("open_price", 0),
            high_price=tick_data.get("high_price", 0),
            low_price=tick_data.get("low_price", 0),
            prev_day_close=tick_data.get("prev_day_close", 0),
            last_trade_timestamp=tick_data.get(
                "last_trade_timestamp", PKDateUtilities.currentDateTimestamp()
            ),
            oi=tick_data.get("oi", 0),
            oi_day_high=tick_data.get("oi_day_high", 0),
            oi_day_low=tick_data.get("oi_day_low", 0),
            exchange_timestamp=tick_data.get(
                "exchange_timestamp", PKDateUtilities.currentDateTimestamp()
            ),
            depth=tick_data.get("depth", {}),
        )
        return tick


def database_writer_worker(args):
    """Worker function for database writer process"""
    data_queue, stop_event, log_level = args
    writer = DatabaseWriterProcess(data_queue, stop_event, log_level)
    writer.run()
