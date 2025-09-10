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
import time
from dataclasses import dataclass
from datetime import datetime
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
    """
    Data class representing a financial instrument from Kite Connect API.

    This class provides a structured representation of instrument data with
    proper type hints and optional fields for flexible data handling.

    Attributes:
        instrument_token (int): Unique identifier for the instrument
        exchange_token (str): Exchange-specific token
        tradingsymbol (str): Trading symbol (e.g., 'RELIANCE', 'INFY')
        name (Optional[str]): Full name of the instrument
        last_price (Optional[float]): Last traded price
        expiry (Optional[str]): Expiry date for derivatives (YYYY-MM-DD format)
        strike (Optional[float]): Strike price for options
        tick_size (float): Minimum price movement
        lot_size (int): Contract/trading lot size
        instrument_type (str): Type of instrument (EQ, FUT, OPT, etc.)
        segment (str): Market segment (NSE, BSE, etc.)
        exchange (str): Exchange name (NSE, BSE, etc.)
        last_updated (str): ISO format timestamp of last update
        nse_stock (bool): Whether this is an NSE-listed stock (default: False)

    Example:
        >>> instrument = Instrument(
        >>>     instrument_token=256265,
        >>>     exchange_token="NSE:256265",
        >>>     tradingsymbol="NIFTY 50",
        >>>     name="Nifty 50 Index",
        >>>     last_price=21500.50,
        >>>     expiry=None,
        >>>     strike=None,
        >>>     tick_size=0.05,
        >>>     lot_size=1,
        >>>     instrument_type="INDEX",
        >>>     segment="NSE",
        >>>     exchange="NSE",
        >>>     last_updated="2023-12-25T10:30:00.000Z",
        >>>     nse_stock=False
        >>> )
    """

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
    Comprehensive manager for Kite Connect instrument data with database integration.

    This class handles the complete lifecycle of instrument data including:
    - Fetching from Kite Connect API
    - Local database storage and caching
    - Intelligent filtering and normalization
    - NSE stock identification and classification
    - Thread-safe database operations

    Features:
    - Automatic database initialization with proper schema
    - Efficient bulk sync operations with rate limiting
    - Support for both local SQLite and remote Turso databases
    - Comprehensive type hints and error handling
    - NSE stock identification using external data source
    - Smart refresh logic based on market hours and data age

    Attributes:
        api_key (str): Kite Connect API key
        access_token (str): Kite Connect access token
        db_path (str): Path to SQLite database file
        local (bool): Whether to use local SQLite database
        recreate_schema (bool): Whether to recreate database schema on init
        base_url (str): Kite Connect API base URL
        logger: Logger instance for debugging and monitoring
        headers (Dict): HTTP headers for API requests

    Example:
        >>> from pkbrokers.kite.instruments import KiteInstruments
        >>>
        >>> # Initialize with API credentials
        >>> instruments = KiteInstruments(
        >>>     api_key="your_api_key",
        >>>     access_token="your_access_token",
        >>>     db_path="/path/to/instruments.db",
        >>>     local=True  # Use local SQLite database
        >>> )
        >>>
        >>> # Sync instruments (fetches if needed)
        >>> success = instruments.sync_instruments()
        >>> if success:
        >>>     # Get all NSE stocks
        >>>     nse_stocks = instruments.get_nse_stocks()
        >>>     print(f"Found {len(nse_stocks)} NSE stocks")
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        db_path: str = os.path.join(DEFAULT_PATH, "instruments.db"),
        local=False,
        recreate_schema=False,
    ):
        """
        Initialize the KiteInstruments manager with API credentials and database configuration.

        Args:
            api_key: Kite Connect API key (required)
            access_token: Kite Connect access token (required)
            db_path: Path to SQLite database file (default: user data directory)
            local: Whether to use local SQLite instead of remote database (default: False)
            recreate_schema: Whether to recreate database schema on initialization (default: True)

        Raises:
            ValueError: If API key or access token are missing or invalid

        Note:
            When using remote database (local=False), ensure TDU and TAT environment variables
            are set for Turso database connection.

        Example:
            >>> # Local SQLite database
            >>> local_instruments = KiteInstruments(
            >>>     api_key="your_api_key",
            >>>     access_token="your_access_token",
            >>>     local=True
            >>> )
            >>>
            >>> # Remote Turso database
            >>> remote_instruments = KiteInstruments(
            >>>     api_key="your_api_key",
            >>>     access_token="your_access_token",
            >>>     local=False  # Uses TDU and TAT environment variables
            >>> )
        """
        self.api_key = api_key
        self.access_token = access_token
        self._update_threshold = 23.5 * 3600  # 23.5 hours in seconds
        self.db_path = db_path
        self.local = local
        self.recreate_schema = recreate_schema
        self.kite_instruments = {}
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

    def _is_after_8_30_am_ist(self, last_updated_str: str) -> bool:
        """
        Check if the given timestamp is after 8:30 AM IST.

        Args:
            last_updated_str: ISO format timestamp string

        Returns:
            bool: True if timestamp is after 8:30 AM IST, False otherwise

        Note:
            Used to determine if data was updated during current trading day.
            Indian stock market opens at 9:15 AM IST, so 8:30 AM is used as
            a conservative threshold for same-day data freshness.
        """
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

    def _is_last_updated_today(self) -> bool:
        """
        Check if the database contains data updated today after 8:30 AM IST.

        Returns:
            bool: True if fresh data exists, False otherwise

        Note:
            This method helps avoid unnecessary API calls when fresh data
            is already available in the database.
        """
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

    def table_exists(self, cursor, table_name):
        cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        """,
            (table_name,),
        )
        return cursor.fetchone() is not None

    def _init_db(self, drop_table: bool = False) -> None:
        """
        Initialize the database schema with proper tables and indexes.

        Args:
            drop_table: Whether to drop existing table and recreate schema

        Note:
            Creates the 'instruments' table with comprehensive schema including:
            - Primary key on (exchange, tradingsymbol, instrument_type)
            - Constraints for data integrity
            - Indexes for performance optimization
            - nse_stock column for NSE stock identification

            Also enables WAL mode for local SQLite databases for better concurrency.
        """
        self.logger.debug("Database initialisation in progress...")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            self.logger.debug("Database connected.")
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

            # Only create if it doesn't exist
            if not self.table_exists(cursor, "instruments"):
                # Create instruments table with constraints and new nse_stock column
                # Use INTEGER instead of BOOLEAN for Turso compatibility
                cursor.execute("""
                    CREATE TABLE instruments (
                        instrument_token INTEGER,
                        exchange_token TEXT,
                        tradingsymbol TEXT, -- NOT NULL,
                        name TEXT,
                        last_price REAL,
                        expiry TEXT,
                        strike REAL,
                        tick_size REAL, -- NOT NULL CHECK(tick_size >= 0),
                        lot_size INTEGER, -- NOT NULL CHECK(lot_size >= 0),
                        instrument_type TEXT, -- NOT NULL,
                        segment TEXT, -- NOT NULL,
                        exchange TEXT, -- NOT NULL,
                        last_updated TEXT DEFAULT (datetime('now')),
                        nse_stock INTEGER DEFAULT 0, -- CHECK (nse_stock IN (0, 1)),
                        PRIMARY KEY (exchange, tradingsymbol, instrument_type)
                    ) -- STRICT
                """)

                # Create optimized indexes including nse_stock
                cursor.execute("""
                    CREATE INDEX idx_instrument_token
                    ON instruments(instrument_token)
                """)
                cursor.execute("""
                    CREATE INDEX idx_tradingsymbol_segment
                    ON instruments(tradingsymbol, segment)
                """)
                cursor.execute("""
                    CREATE INDEX idx_nse_stock
                    ON instruments(nse_stock)
                """)

                conn.commit()
            self.logger.debug("Database initialised for table instruments.")

    def _get_connection(self, local: bool = False) -> sqlite3.Connection:
        """
        Get a thread-safe database connection.

        Args:
            local: Force local connection even if configured for remote

        Returns:
            sqlite3.Connection: Database connection object

        Note:
            Returns either local SQLite connection or remote Turso connection
            based on configuration and the 'local' parameter.
        """
        if local or self.local:
            return sqlite3.connect(self.db_path, timeout=30)
        else:
            return libsql.connect(
                database=PKEnvironment().TDU, auth_token=PKEnvironment().TAT
            )

    def _get_nse_trading_symbols(self) -> list[str]:
        """
        Get NSE trading symbols from NSE data fetcher with caching.

        Returns:
            list[str]: List of NSE trading symbols

        Note:
            Uses nseStockDataFetcher to fetch Nifty 50 and F&O symbols.
            Results are cached for subsequent calls to avoid repeated fetching.
        """
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
        Determine if instrument data needs to be refreshed.

        Returns:
            bool: True if refresh is needed, False otherwise

        Checks:
            1. If database is empty (first run)
            2. If data is older than update threshold (23.5 hours)
            3. If last update was not today

        Note:
            The 23.5 hour threshold ensures daily refresh while accounting for
            market timing variations.
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
        """
        Convert raw API CSV data to structured Instrument object.

        Args:
            raw: Dictionary of raw CSV data from Kite API

        Returns:
            Optional[Instrument]: Normalized Instrument object or None if invalid

        Note:
            Performs data validation, type conversion, and NSE stock identification.
            Invalid instruments are logged and skipped.
        """
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
        """
        Standardize expiry date format to YYYY-MM-DD.

        Args:
            expiry: Raw expiry date string

        Returns:
            Optional[str]: Normalized expiry date or None if invalid
        """
        if not expiry:
            return None
        try:
            return datetime.strptime(expiry, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            self.logger.warn(f"Invalid expiry format: {expiry}")
            return None

    def _filter_instrument(self, instrument: Instrument) -> bool:
        """
        Filter instruments based on criteria for equity trading.

        Args:
            instrument: Instrument object to filter

        Returns:
            bool: True if instrument should be included, False otherwise

        Filter Criteria:
            - NSE exchange and segment
            - Equity instrument type (EQ)
            - Valid name
            - Either in NSE symbol list or is Nifty 50/Sensex index
        """
        nse_symbols = self._get_nse_trading_symbols()

        # Include all NSE INDICES and BSE SENSEX
        indices_conditions = (
            instrument.exchange == "NSE"
            and instrument.segment == "INDICES"
            and instrument.instrument_type == "EQ"
        ) or instrument.instrument_token in [NIFTY_50, BSE_SENSEX]
        basic_conditions = (
            instrument.exchange == "NSE"
            and instrument.segment == "NSE"
            and instrument.instrument_type == "EQ"
            and instrument.name is not None
        )
        if nse_symbols:
            # Use NSE symbol list as the primary filter
            in_nse_symbols_or_indices = (basic_conditions and (
                instrument.tradingsymbol.replace("-BE", "").replace("-BZ", "")
                in nse_symbols) or indices_conditions
            )
            if in_nse_symbols_or_indices:
                self._filtered_trading_symbols.append(
                    instrument.tradingsymbol.replace("-BE", "").replace("-BZ", "")
                )
                self.kite_instruments[instrument.instrument_token] = instrument
            else:
                # if basic_conditions:
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
        """
        Fetch instruments from Kite Connect API.

        Returns:
            List[Instrument]: List of normalized instrument objects

        Raises:
            requests.exceptions.RequestException: If API call fails
            Exception: For unexpected processing errors

        Note:
            Fetches CSV data from Kite API and converts to structured objects.
            Includes error handling and logging for failed requests.
        """
        if self.kite_instruments is not None and len(self.kite_instruments.keys()) > 0:
            return self.kite_instruments.values()

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
            filtered_instruments = [
                inst for inst in instruments if self._filter_instrument(inst)
            ]
            # self.logger.debug(
            #     f"Filtered out but present in NSE_symbols:{set(self._nse_trading_symbols) - set(self._filtered_trading_symbols)}"
            # )
            return filtered_instruments

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch instruments: {str(e)}")
            raise
        # except UnicodeEncodeError:
        #     from pkbrokers.kite.examples.pkkite import try_refresh_token
        #     try_refresh_token()
        #     self.headers = {
        #         "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        #         "X-Kite-Version": "3",
        #         "Authorization": f"token {self.api_key}:{PKEnvironment().KTOKEN}",
        #     }
        except Exception as e:
            self.logger.error(f"Unexpected error processing instruments: {str(e)}")
            raise

    def _store_instruments(self, instruments: List[Instrument]) -> None:
        """
        Bulk upsert instruments into database with efficient batch operations.

        Args:
            instruments: List of Instrument objects to store

        Note:
            Filters instruments before storage and uses batch operations for
            better performance. Handles both new inserts and updates.
        """
        if not instruments:
            self.logger.warn("No instruments to store")
            return

        self.logger.info(f"Updating/Inserting {len(instruments)} instruments")
        # self.logger.debug(
        #     f"Filtered out but present in NSE_symbols:{set(self._nse_trading_symbols) - set(instruments)}"
        # )
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
                for i in instruments
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
            self.logger.info(f"Stored/updated {len(data)} instruments")

    def sync_instruments(
        self, instruments: List[Instrument] = [], force_fetch: bool = True
    ) -> bool:
        """
        Complete instrument synchronization workflow.

        Args:
            instruments: Pre-fetched instruments (optional)
            force_fetch: Whether to force API fetch if instruments provided (default: True)

        Returns:
            bool: True if sync successful, False otherwise

        Workflow:
            1. Check if refresh is needed
            2. Initialize database schema
            3. Fetch instruments from API if needed
            4. Store filtered instruments in database
        """
        try:
            if self._needs_refresh():
                self.logger.debug("Starting instruments sync")
                begin_time = time.time()
                self._init_db(drop_table=True)
                instruments = self.fetch_instruments() if force_fetch else instruments
                self._store_instruments(instruments)
                self.logger.info(
                    f"Synced NSE Instruments in: {'%.3f' % (time.time() - begin_time)} sec."
                )
            return True
        except Exception as e:
            self.logger.error(f"Sync failed: {str(e)}")
            return False

    def get_instrument_count(self) -> int:
        """
        Get total count of instruments in database.

        Returns:
            int: Number of instruments stored
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(1) FROM instruments")
            return cursor.fetchone()[0]

    def get_nse_stock_count(self) -> int:
        """
        Get count of instruments marked as NSE stocks.

        Returns:
            int: Number of NSE stocks in database
        """
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
        Get equity instruments with dynamic column selection and filtering.

        Args:
            column_names: Comma-separated list of column names to retrieve
            segment: Market segment to filter by (default: "NSE")
            only_nse_stocks: Whether to return only NSE-listed stocks (default: False)

        Returns:
            List[Dict]: List of instrument dictionaries with requested columns

        Raises:
            ValueError: If invalid column names are requested

        Example:
            >>> # Get basic instrument info
            >>> equities = instruments.get_equities(
            >>>     column_names="instrument_token,tradingsymbol,name"
            >>> )
            >>>
            >>> # Get only NSE stocks with price data
            >>> nse_stocks = instruments.get_equities(
            >>>     column_names="instrument_token,tradingsymbol,name,last_price",
            >>>     only_nse_stocks=True
            >>> )
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

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(zip(requested_columns, row)) for row in cursor.fetchall()]

    def get_or_fetch_instrument_tokens(
        self, all_columns: bool = True, only_nse_stocks: bool = False
    ) -> List[int]:
        """
        Get instrument tokens, fetching from API if database is empty.

        Args:
            all_columns: Whether to return full instrument data or just tokens
            only_nse_stocks: Whether to return only NSE stock tokens

        Returns:
            List[int]: List of instrument tokens or full instrument data

        Note:
            Automatically triggers sync if no instruments are found in database.
        """
        equities_count = self.get_instrument_count()
        self.logger.debug(f"Total instrument token count received:{equities_count}")
        if equities_count == 0:
            self.sync_instruments(force_fetch=True)
        equities = self.get_equities(
            column_names="instrument_token"
            if not all_columns
            else "instrument_token,tradingsymbol,name",
            only_nse_stocks=only_nse_stocks,
        )
        tokens = self.get_instrument_tokens(equities=equities)
        self.logger.debug(f"All tokens received:{tokens}")
        return tokens

    def get_instrument_tokens(self, equities: List[Dict]) -> List[int]:
        """
        Safely extract instrument tokens from equity data.

        Args:
            equities: List of instrument dictionaries

        Returns:
            List[int]: List of validated instrument tokens

        Note:
            Handles missing or invalid token values gracefully.
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
        """
        Get complete instrument data by token.

        Args:
            instrument_token: Instrument token to lookup

        Returns:
            Optional[Dict]: Complete instrument data or None if not found

        Note:
            Returns all columns including nse_stock as boolean for easier use.
        """
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
        """
        Convenience method to get all NSE-listed stocks.

        Returns:
            List[Dict]: List of NSE stock instruments with nse_stock as boolean
        """
        stocks = self.get_equities(
            column_names="instrument_token,tradingsymbol,name,last_price,nse_stock",
            only_nse_stocks=True,
        )
        # Convert nse_stock to boolean for easier use
        for stock in stocks:
            stock["nse_stock"] = bool(stock["nse_stock"])
        return stocks

    def update_nse_stock_status(self, tradingsymbol: str, is_nse_stock: bool) -> bool:
        """
        Update the NSE stock status for a specific instrument.

        Args:
            tradingsymbol: Trading symbol to update
            is_nse_stock: Whether the instrument is an NSE stock

        Returns:
            bool: True if update successful, False otherwise
        """
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

        Returns:
            bool: True if migration successful, False otherwise

        Note:
            This should be called once after adding the nse_stock column to
            populate it based on the NSE symbol list.
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
