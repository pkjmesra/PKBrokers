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

"""
Kite Connect Instruments Manager

Handles instrument data synchronization and querying from Zerodha's Kite Connect API
"""
import csv
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Dict, List, Optional

import libsql
import pytz
import requests
from PKDevTools.classes import Archiver
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities
from PKNSETools.PKNSEStockDataFetcher import nseStockDataFetcher

# Configure logging
DEFAULT_PATH = Archiver.get_user_data_dir()
NIFTY_50 = 256265
BSE_SENSEX = 265


@dataclass
class Instrument:
    """Data class representing a financial instrument"""

    instrument_token: int
    exchange_token: str
    tradingsymbol: str
    name: Optional[str]
    last_price: Optional[float]
    expiry: Optional[str]
    strike: Optional[float]
    tick_size: float
    lot_size: int
    instrument_type: str
    segment: str
    exchange: str
    last_updated: str
    nse_stock: bool = False  # New field to indicate if it's an NSE stock


class KiteInstruments:
    """
    Manages instrument data from Kite Connect API

    Features:
    - Automatic database initialization
    - Efficient bulk sync operations
    - Thread-safe queries
    - Comprehensive type hints
    - NSE stock identification
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        db_path: str = os.path.join(DEFAULT_PATH, "instruments.db"),
        local=False,
        recreate_schema=True,
    ):
        """
        Initialize instruments manager

        Args:
            api_key: Kite Connect API key
            access_token: Kite Connect access token
            db_path: Path to SQLite database file
        """
        self.api_key = api_key
        self.access_token = access_token
        self._update_threshold = 23.5 * 3600  # 23.5 hours in seconds
        self.db_path = db_path
        self.local = local
        self.recreate_schema = recreate_schema
        self.base_url = "https://api.kite.trade"
        self.logger = default_logger()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}",
        }
        self._nse_trading_symbols: Optional[list[str]] = None
        self._filtered_trading_symbols: Optional[list[str]] = []
        self._init_db(drop_table=recreate_schema)

    def _is_after_8_30_am_ist(self, last_updated_str):
        # Parse the datetime string
        last_updated = datetime.fromisoformat(last_updated_str.replace("Z", "+00:00"))

        # Convert to IST timezone
        ist_timezone = pytz.timezone("Asia/Kolkata")
        last_updated_ist = last_updated.astimezone(ist_timezone)

        # Create 8:30 AM IST time for the same date
        eight_thirty_am_ist = datetime.combine(
            PKDateUtilities.currentDateTime().date(), time(8, 30, 0)
        ).replace(tzinfo=ist_timezone)

        # Check if last_updated is >= 8:30 AM IST
        return last_updated_ist >= eight_thirty_am_ist

    def _is_last_updated_today(self):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT last_updated FROM instruments limit 1")
                rows = cursor.fetchall()
                is_after_830 = False
                for row in rows:
                    last_updated_str = row[0]
                    is_after_830 = self._is_after_8_30_am_ist(last_updated_str)
                    break
                return is_after_830
        except BaseException:
            return False

    def _init_db(self, drop_table=False) -> None:
        """Initialize database schema with proper indexes"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            if (
                drop_table
                and not self._is_last_updated_today()
                and self._needs_refresh()
            ):
                self.logger.debug("Dropping table instruments.")
                cursor.execute("DROP TABLE IF EXISTS instruments")

            if self.local:
                self.logger.debug("Running in local database mode.")
                # Enable WAL mode for better concurrency
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")

            # Create instruments table with constraints and new nse_stock column
            # Use INTEGER instead of BOOLEAN for Turso compatibility
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS instruments (
                    instrument_token INTEGER,
                    exchange_token TEXT,
                    tradingsymbol TEXT NOT NULL,
                    name TEXT,
                    last_price REAL,
                    expiry TEXT,
                    strike REAL,
                    tick_size REAL NOT NULL CHECK(tick_size >= 0),
                    lot_size INTEGER NOT NULL CHECK(lot_size >= 0),
                    instrument_type TEXT NOT NULL,
                    segment TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    last_updated TEXT DEFAULT (datetime('now')),
                    nse_stock INTEGER DEFAULT 0 CHECK (nse_stock IN (0, 1)),
                    PRIMARY KEY (exchange, tradingsymbol, instrument_type)
                ) STRICT
            """)

            # Create optimized indexes including nse_stock
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_instrument_token
                ON instruments(instrument_token)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_tradingsymbol_segment
                ON instruments(tradingsymbol, segment)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_nse_stock
                ON instruments(nse_stock)
            """)

            conn.commit()
            self.logger.debug("Database initialised for table instruments.")

    def _get_connection(self, local=False) -> sqlite3.Connection:
        """Get thread-safe database connection"""
        if local or self.local:
            return sqlite3.connect(self.db_path, timeout=30)
        else:
            return libsql.connect(
                database=PKEnvironment().TDU, auth_token=PKEnvironment().TAT
            )

    def _get_nse_trading_symbols(self) -> list[str]:
        """Get NSE trading symbols from NSE data fetcher with caching"""
        if self._nse_trading_symbols is None:
            try:
                nse_fetcher = nseStockDataFetcher()
                equities = list(set(nse_fetcher.fetchNiftyCodes(tickerOption=12)))
                fno = list(set(nse_fetcher.fetchNiftyCodes(tickerOption=14)))

                self._nse_trading_symbols = list(set(equities + fno))
                self.logger.debug(
                    f"Fetched {len(self._nse_trading_symbols)} Unique NSE symbols"
                )
                self.logger.debug(f"{self._nse_trading_symbols}")
            except Exception as e:
                self.logger.warn(f"Failed to fetch NSE symbols: {str(e)}")
                self._nse_trading_symbols = []

        return self._nse_trading_symbols

    def _needs_refresh(self) -> bool:
        """
        Determines if instruments need refresh with:
        - Database timestamp check
        - Minimum update frequency enforcement
        - Network-optimized queries
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Single efficient query that returns age in seconds
            try:
                cursor.execute(
                    """
                    SELECT CASE
                        WHEN NOT EXISTS (SELECT 1 FROM instruments LIMIT 1) THEN 1
                        WHEN (
                            strftime('%s','now') -
                            COALESCE(
                                (SELECT strftime('%s',MAX(last_updated)) FROM instruments),
                                0
                            ) > ?
                        ) THEN 1
                        WHEN (
                            SELECT DATE(MAX(last_updated),'utc')
                            FROM instruments
                        ) != DATE('now','utc') THEN 1
                        ELSE 0
                    END AS needs_refresh
                """,
                    (self._update_threshold,),
                )

                return cursor.fetchone()[0] == 1  # 0 for first run case
            except BaseException:
                return True

    def _normalize_instrument(self, raw: Dict[str, str]) -> Optional[Instrument]:
        """Convert raw API data to Instrument object with validation"""
        try:
            # Check if this is an NSE stock
            nse_symbols = self._get_nse_trading_symbols()
            tradingsymbol = raw["tradingsymbol"].strip()
            is_nse_stock = tradingsymbol in nse_symbols if nse_symbols else False

            return Instrument(
                instrument_token=int(raw["instrument_token"]),
                exchange_token=raw["exchange_token"],
                tradingsymbol=tradingsymbol,
                name=raw["name"].strip() if raw.get("name") else None,
                last_price=float(raw["last_price"]) if raw.get("last_price") else None,
                expiry=self._normalize_expiry(raw.get("expiry")),
                strike=float(raw["strike"]) if raw.get("strike") else None,
                tick_size=float(raw["tick_size"]),
                lot_size=int(raw["lot_size"]),
                instrument_type=raw["instrument_type"].strip(),
                segment=raw["segment"].strip(),
                exchange=raw["exchange"].strip(),
                last_updated=datetime.now().isoformat(),
                nse_stock=is_nse_stock,
            )
        except (ValueError, KeyError) as e:
            self.logger.warn(f"Skipping malformed instrument: {str(e)}")
            return None

    def _normalize_expiry(self, expiry: Optional[str]) -> Optional[str]:
        """Standardize expiry date format"""
        if not expiry:
            return None
        try:
            return datetime.strptime(expiry, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            self.logger.warn(f"Invalid expiry format: {expiry}")
            return None

    def _filter_instrument(self, instrument: Instrument) -> bool:
        """
        Filter instruments based on criteria, with NSE symbol list preference

        If NSE trading symbols are available, use them for filtering.
        Otherwise, fall back to the original filtering logic.
        """
        nse_symbols = self._get_nse_trading_symbols()

        basic_conditions = (
            instrument.exchange == "NSE"
            and instrument.segment == "NSE"
            and instrument.instrument_type == "EQ"
            and instrument.name is not None
        )
        if nse_symbols:
            # Use NSE symbol list as the primary filter
            in_nse_symbols_or_indices = basic_conditions and (
                instrument.tradingsymbol.replace("-BE", "").replace("-BZ", "")
                in nse_symbols
                or instrument.instrument_token in [NIFTY_50, BSE_SENSEX]
            )
            if in_nse_symbols_or_indices:
                self._filtered_trading_symbols.append(
                    instrument.tradingsymbol.replace("-BE", "").replace("-BZ", "")
                )
            else:
                if basic_conditions:
                    self.logger.debug(f"Filtered Out:{instrument.tradingsymbol}")
            return in_nse_symbols_or_indices

        else:
            # Fall back to original filtering logic
            return (
                basic_conditions
                and "-" not in instrument.tradingsymbol
                and 1 <= instrument.lot_size <= 100
                and "ETF" not in instrument.tradingsymbol
                and instrument.tradingsymbol[0].isupper()
                if instrument.tradingsymbol
                else False
            )

    def fetch_instruments(self) -> List[Instrument]:
        """Fetch instruments from Kite API"""
        url = f"{self.base_url}/instruments/NSE"
        self.logger.debug(f"Fetching instruments from {url}")

        try:
            self.logger.debug(f"Fetching from {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            content = response.content.decode("utf-8")
            reader = csv.DictReader(content.splitlines())

            instruments = []
            for row in reader:
                instrument = self._normalize_instrument(row)
                if instrument:
                    instruments.append(instrument)

            self.logger.debug(f"Fetched {len(instruments)} valid instruments")
            return instruments

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch instruments: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error processing instruments: {str(e)}")
            raise

    def store_instruments(self, instruments: List[Instrument]) -> None:
        """Bulk upsert instruments into database with nse_stock column"""
        if not instruments:
            self.logger.warn("No instruments to store")
            return

        filtered_instruments = [
            inst for inst in instruments if self._filter_instrument(inst)
        ]
        self.logger.debug(f"Updating/Inserting {len(filtered_instruments)} instruments")
        self.logger.debug(
            f"Filtered out but present in NSE_symbols:{set(self._nse_trading_symbols) - set(self._filtered_trading_symbols)}"
        )
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Prepare batch data including nse_stock column (as INTEGER)
            data = [
                (
                    i.instrument_token,
                    i.exchange_token,
                    i.tradingsymbol,
                    i.name,
                    i.last_price,
                    i.expiry,
                    i.strike,
                    i.tick_size,
                    i.lot_size,
                    i.instrument_type,
                    i.segment,
                    i.exchange,
                    datetime.now().isoformat(),
                    1 if i.nse_stock else 0,  # nse_stock column as INTEGER
                )
                for i in filtered_instruments
            ]

            if self.recreate_schema:
                cursor.executemany(
                    """
                    INSERT or IGNORE INTO instruments
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    data,
                )
            else:
                # Efficient bulk upsert including nse_stock column
                cursor.executemany(
                    """
                    INSERT INTO instruments
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(exchange, tradingsymbol, instrument_type)
                    DO UPDATE SET
                        instrument_token = excluded.instrument_token,
                        exchange_token = excluded.exchange_token,
                        name = excluded.name,
                        last_price = excluded.last_price,
                        tick_size = excluded.tick_size,
                        lot_size = excluded.lot_size,
                        segment = excluded.segment,
                        last_updated = datetime('now'),
                        nse_stock = excluded.nse_stock
                """,
                    data,
                )

            conn.commit()
            self.logger.debug(f"Stored/updated {len(data)} instruments")

    def sync_instruments(self, instruments=[], force_fetch: bool = True) -> bool:
        """Complete sync workflow"""
        try:
            if self._needs_refresh():
                self.logger.debug("Starting instruments sync")
                self._init_db(drop_table=True)
                instruments = self.fetch_instruments() if force_fetch else instruments
                self.store_instruments(instruments)
            return True
        except Exception as e:
            self.logger.error(f"Sync failed: {str(e)}", exc_info=True)
            return False

    def get_instrument_count(self) -> int:
        """Get total instrument count"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(1) FROM instruments")
            return cursor.fetchone()[0]

    def get_nse_stock_count(self) -> int:
        """Get count of instruments marked as NSE stocks"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(1) FROM instruments WHERE nse_stock = 1")
            return cursor.fetchone()[0]

    def get_equities(
        self,
        column_names: str = "instrument_token,tradingsymbol,name",
        segment: str = "NSE",
        only_nse_stocks: bool = False,
    ) -> List[Dict]:
        """
        Get equity instruments with dynamic column selection

        Args:
            column_names: Comma-separated list of valid column names
            segment: Market segment (NSE, INDICES, etc.)
            only_nse_stocks: Whether to return only instruments marked as NSE stocks

        Returns:
            List of instrument dictionaries with requested columns

        Raises:
            ValueError: If invalid column names are provided
        """
        # Validate and sanitize column names
        valid_columns = {
            "instrument_token",
            "exchange_token",
            "tradingsymbol",
            "name",
            "last_price",
            "expiry",
            "strike",
            "tick_size",
            "lot_size",
            "instrument_type",
            "segment",
            "exchange",
            "last_updated",
            "nse_stock",
        }
        column_names = column_names.replace(" ", "").strip()
        requested_columns = [col.strip() for col in column_names.split(",")]
        invalid_columns = set(requested_columns) - valid_columns

        if invalid_columns:
            raise ValueError(f"Invalid columns requested: {invalid_columns}")

        # Build safe SQL query
        columns_sql = ", ".join(requested_columns)
        params = []
        query = f"""
            SELECT {columns_sql} FROM instruments
            """
        # """
        #     WHERE
        #         exchange = 'NSE' AND
        #         segment = ? AND
        #         instrument_type = 'EQ'
        # """

        # # Add NSE stock filter if requested
        # params = [segment]
        # if only_nse_stocks:
        #     query += " AND nse_stock = 1"

        # # Add segment-specific filters
        # if segment == "NSE":
        #     query = (
        #         query
        #         + """
        #             AND
        #             tradingsymbol NOT LIKE '%ETF%' AND  -- Exclude ETFs
        #             name IS NOT NULL AND
        #             tradingsymbol NOT LIKE '%-%' AND  -- Exclude preferred stocks
        #             lot_size BETWEEN 1 AND 100 AND
        #             tradingsymbol GLOB '[A-Z]*'  -- Starts with uppercase letter
        #         ORDER BY tradingsymbol
        #     """
        #     )

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(zip(requested_columns, row)) for row in cursor.fetchall()]

    def get_or_fetch_instrument_tokens(self, all_columns=False, only_nse_stocks=False):
        equities_count = self.get_instrument_count()
        if equities_count == 0:
            self.sync_instruments(force_fetch=True)
        equities = self.get_equities(
            column_names="instrument_token"
            if not all_columns
            else "instrument_token,tradingsymbol,name",
            only_nse_stocks=only_nse_stocks,
        )
        tokens = self.get_instrument_tokens(equities=equities)
        return tokens

    def get_instrument_tokens(self, equities: List[Dict]) -> List[int]:
        """
        Safely extracts instrument tokens with validation

        Features:
        - Type checking
        - Handles missing keys
        - Filters invalid tokens
        - Preserves order
        """
        tokens = []
        for eq in equities:
            try:
                token = int(eq["instrument_token"])
                tokens.append(token)
            except (KeyError, ValueError, TypeError):
                continue
        return tokens

    def get_instrument(self, instrument_token: int) -> Optional[Dict]:
        """Get single instrument by token"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM instruments
                WHERE instrument_token = ?
            """,
                (instrument_token,),
            )
            row = cursor.fetchone()
            if row:
                columns = [
                    "instrument_token",
                    "exchange_token",
                    "tradingsymbol",
                    "name",
                    "last_price",
                    "expiry",
                    "strike",
                    "tick_size",
                    "lot_size",
                    "instrument_type",
                    "segment",
                    "exchange",
                    "last_updated",
                    "nse_stock",
                ]
                result = dict(zip(columns, row))
                # Convert nse_stock to boolean for easier use
                result["nse_stock"] = bool(result["nse_stock"])
                return result
            return None

    def get_nse_stocks(self) -> List[Dict]:
        """Convenience method to get all NSE stocks"""
        stocks = self.get_equities(
            column_names="instrument_token,tradingsymbol,name,last_price,nse_stock",
            only_nse_stocks=True,
        )
        # Convert nse_stock to boolean for easier use
        for stock in stocks:
            stock["nse_stock"] = bool(stock["nse_stock"])
        return stocks

    def update_nse_stock_status(self, tradingsymbol: str, is_nse_stock: bool) -> bool:
        """Update the nse_stock status for a specific instrument"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE instruments
                    SET nse_stock = ?, last_updated = datetime('now')
                    WHERE tradingsymbol = ? AND exchange = 'NSE'
                """,
                    (1 if is_nse_stock else 0, tradingsymbol),
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            self.logger.error(f"Failed to update NSE stock status: {str(e)}")
            return False

    def migrate_to_nse_stock_column(self) -> bool:
        """
        Migrate existing data to use the nse_stock column.
        This should be called once after adding the new column.
        """
        try:
            nse_symbols = self._get_nse_trading_symbols()
            if not nse_symbols:
                self.logger.warn("No NSE symbols available for migration")
                return False

            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Update all instruments based on NSE symbol list
                cursor.execute(
                    """
                    UPDATE instruments
                    SET nse_stock = 1, last_updated = datetime('now')
                    WHERE exchange = 'NSE'
                    AND segment = 'NSE'
                    AND instrument_type = 'EQ'
                    AND tradingsymbol IN ({})
                    """.format(",".join(["?"] * len(nse_symbols))),
                    list(nse_symbols),
                )

                # Set nse_stock = 0 for non-matching instruments
                cursor.execute(
                    """
                    UPDATE instruments
                    SET nse_stock = 0, last_updated = datetime('now')
                    WHERE exchange = 'NSE'
                    AND segment = 'NSE'
                    AND instrument_type = 'EQ'
                    AND nse_stock IS NULL
                    """
                )

                conn.commit()
                self.logger.debug(
                    f"Migrated {cursor.rowcount} instruments to use nse_stock column"
                )
                return True

        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            return False
