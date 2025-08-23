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
import sqlite3
import threading
from contextlib import contextmanager

from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger

DEFAULT_PATH = Archiver.get_user_data_dir()


class ThreadSafeDatabase:
    def __init__(self, db_path=os.path.join(DEFAULT_PATH, "ticks.db")):
        self.db_path = db_path
        self.local = threading.local()  # This creates thread-local storage
        self.lock = threading.Lock()
        # Configure logging
        self.logger = default_logger()
        self._initialize_db()

    def _initialize_db(self, force_drop=False):
        """Initialize database schema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Drop old table if exists
            if force_drop:
                self.logger.debug("Dropping tables market_depth, ticks.")
                cursor.execute("DROP TABLE IF EXISTS market_depth")
                cursor.execute("DROP TABLE IF EXISTS ticks")
            # Enable strict datetime typing
            cursor.execute("PRAGMA strict=ON")
            # Main ticks table with composite primary key
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    instrument_token INTEGER,
                    timestamp DATETIME, -- Will use registered converter
                    last_price REAL,
                    day_volume INTEGER,
                    oi INTEGER,
                    buy_quantity INTEGER,
                    sell_quantity INTEGER,
                    high_price REAL,
                    low_price REAL,
                    open_price REAL,
                    prev_day_close REAL,
                    PRIMARY KEY (instrument_token)
                ) WITHOUT ROWID  -- Better for PK-based lookups
            """)

            # Market depth table with foreign key relationship
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_depth (
                    instrument_token INTEGER,
                    timestamp DATETIME, -- Will use registered converter
                    depth_type TEXT CHECK(depth_type IN ('bid', 'ask')),
                    position INTEGER CHECK(position BETWEEN 1 AND 5),
                    price REAL,
                    quantity INTEGER,
                    orders INTEGER,
                    PRIMARY KEY (instrument_token, depth_type, position),
                    FOREIGN KEY (instrument_token)
                        REFERENCES ticks(instrument_token)
                        ON DELETE CASCADE
                ) WITHOUT ROWID
            """)

            # Indexes for faster queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_depth_main
                ON market_depth(instrument_token)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp ON ticks(timestamp);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_instrument ON ticks(instrument_token);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS instrument_last_update (
                    instrument_token INTEGER PRIMARY KEY,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS update_timestamp_insert
                AFTER INSERT ON ticks
                FOR EACH ROW
                BEGIN
                    INSERT INTO instrument_last_update (instrument_token, last_updated)
                    VALUES (NEW.instrument_token, CURRENT_TIMESTAMP)
                    ON CONFLICT(instrument_token) DO UPDATE
                    SET last_updated = CURRENT_TIMESTAMP;
                END;
            """)
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS update_timestamp_update
                AFTER UPDATE ON ticks
                FOR EACH ROW
                BEGIN
                    INSERT INTO instrument_last_update (instrument_token, last_updated)
                    VALUES (NEW.instrument_token, CURRENT_TIMESTAMP)
                    ON CONFLICT(instrument_token) DO UPDATE
                    SET last_updated = CURRENT_TIMESTAMP;
                END;
            """)
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous = NORMAL")
            cursor.execute("PRAGMA cache_size = -70000")  # 70MB cache
            conn.commit()

    def close_all(self):
        """Close all thread connections"""
        if hasattr(self.local, "conn"):
            self.local.conn.close()

    @contextmanager
    def get_connection(self):
        """Get a thread-local database connection"""
        if not hasattr(self.local, "conn"):
            self.local.conn = sqlite3.connect(self.db_path, timeout=30)
            self.local.conn.execute(
                "PRAGMA journal_mode=WAL"
            )  # Better for concurrent access

        try:
            yield self.local.conn
        except Exception as e:
            self.local.conn.rollback()
            raise e

    def insert_ticks(self, ticks):
        """Thread-safe batch insert with market depth"""
        if not ticks:
            return

        with self.lock, self.get_connection() as conn:
            try:
                # Prepare tick data (tuples are faster than dicts)
                tick_data = [
                    (
                        t["instrument_token"],
                        t["timestamp"],
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
                # Batch upsert for ticks
                conn.executemany(
                    """
                    INSERT INTO ticks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instrument_token) DO UPDATE SET
                        timestamp = excluded.timestamp,
                        last_price = excluded.last_price,
                        day_volume = excluded.day_volume,
                        oi = excluded.oi,
                        buy_quantity = excluded.buy_quantity,
                        sell_quantity = excluded.sell_quantity,
                        high_price = excluded.high_price,
                        low_price = excluded.low_price,
                        open_price = excluded.open_price,
                        prev_day_close = excluded.prev_day_close
                """,
                    tick_data,
                )

                # Insert market depth data
                depth_data = []
                for tick in ticks:
                    if "depth" in tick:
                        ts = tick["timestamp"]
                        inst = tick["instrument_token"]

                        # Process bids (position 1-5)
                        for i, bid in enumerate(tick["depth"]["bid"][:5], 1):
                            depth_data.append(
                                (
                                    inst,
                                    ts,
                                    "bid",
                                    i,
                                    bid["price"],
                                    bid["quantity"],
                                    bid["orders"],
                                )
                            )

                        # Process asks (position 1-5)
                        for i, ask in enumerate(tick["depth"]["ask"][:5], 1):
                            depth_data.append(
                                (
                                    inst,
                                    ts,
                                    "ask",
                                    i,
                                    ask["price"],
                                    ask["quantity"],
                                    ask["orders"],
                                )
                            )

                if depth_data:
                    # Efficient batch upsert using executemany
                    conn.executemany(
                        """
                        INSERT INTO market_depth (
                            instrument_token, timestamp, depth_type,
                            position, price, quantity, orders
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(instrument_token, depth_type, position)
                        DO UPDATE SET
                            timestamp = excluded.timestamp,
                            price = excluded.price,
                            quantity = excluded.quantity,
                            orders = excluded.orders
                    """,
                        depth_data,
                    )

                conn.commit()
                self.logger.debug(f"Inserted {len(ticks)} ticks.")
            except sqlite3.OperationalError as e:
                self.logger.error(
                    f"Reinitializing Database. Database Insert error: {str(e)}"
                )
                conn.rollback()
                self.close_all()
                self._initialize_db(force_drop=True)
            except Exception as e:
                self.logger.error(f"Database insert error: {str(e)}")
                conn.rollback()
