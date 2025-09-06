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
import time
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Dict, List, Union

import libsql
import requests
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities

MAX_CANDLES_COUNT = 365


class Historical_Interval(Enum):
    """
    Enumeration of supported historical data intervals for Kite Connect API.

    This enum provides standardized interval values for fetching historical
    market data at different time resolutions.

    Attributes:
        day: Daily interval (1 day)
        min_1: 1-minute interval
        min_5: 5-minute interval
        min_10: 10-minute interval
        min_15: 15-minute interval
        min_30: 30-minute interval
        min_60: 60-minute interval

    Example:
        >>> from pkbrokers.kite.instrumentHistory import Historical_Interval
        >>> interval = Historical_Interval.min_5
        >>> print(interval.value)  # "5minute"
        >>> # Use in API calls
        >>> data = kite_history.get_historical_data(256265, interval=interval.value)
    """

    day = "day"
    min_1 = "minute"
    min_5 = "5minute"
    min_10 = "10minute"
    min_15 = "15minute"
    min_30 = "30minute"
    min_60 = "60minute"


class KiteTickerHistory:
    """
    Fetches historical data from Zerodha's Kite Connect API with comprehensive features.

    This class provides robust historical data retrieval with:
    - Proper authentication and cookie handling
    - Strict rate limiting (3 requests/second as per Kite API limits)
    - Batch processing with automatic retries and error handling
    - SQLite database integration for caching and persistence
    - Support for multiple time intervals via Historical_Interval enum

    The class handles both API data fetching and local database storage, providing
    efficient data retrieval while respecting API rate limits.

    # Attributes:
        BASE_URL (str): Kite Connect API base URL for historical data
        RATE_LIMIT (int): Maximum requests per second (3 as per Kite API limits)
        RATE_LIMIT_WINDOW (float): Rate limiting window in seconds
        enctoken (str): Authentication token for Kite API
        user_id (str): Zerodha user ID
        session (requests.Session): HTTP session with authentication headers
        last_request_time (float): Timestamp of last API request for rate limiting
        lock (Lock): Thread lock for rate limiting synchronization
        failed_tokens (List[int]): List of instrument tokens that failed to fetch
        db_conn: Database connection for data caching

    # Limits
        The Zerodha Kite Connect Historical Data API has specific limitations on
        the maximum number of days of historical data that can be fetched in a
        single request, depending on the chosen interval. These limits are:

        Minute intervals (1, 2-minute): 60 days
        Minute intervals (3, 4, 5, 10-minute): 100 days
        Minute intervals (15, 30-minute): 200 days
        Hourly intervals (60-minute, hour, 2, 3, 4-hour): 400 days
        Daily and Weekly intervals: 2000 days

        While there are limits per request, it is possible to retrieve historical
        data beyond these limits by making multiple requests and stitching the data
        together. For example, to get 1-minute data for a year, one would need to
        make multiple requests, each fetching a maximum of 60 days of data.

    # Example:
        >>> from pkbrokers.kite.instrumentHistory import KiteTickerHistory, Historical_Interval
        >>> from pkbrokers.kite.authenticator import KiteAuthenticator
        >>>
        >>> # Authenticate first
        >>> authenticator = KiteAuthenticator()
        >>> enctoken = authenticator.get_enctoken()
        >>>
        >>> # Create history client
        >>> history = KiteTickerHistory(
        >>>     enctoken=enctoken,
        >>>     user_id="YourUserId",
        >>>     access_token_response=authenticator.access_token_response
        >>> )
        >>>
        >>> # Fetch historical data
        >>> data = history.get_historical_data(
        >>>     instrument_token=256265,
        >>>     interval=Historical_Interval.day.value,
        >>>     from_date="2023-01-01",
        >>>     to_date="2023-12-31"
        >>> )
        >>>
        >>> # Fetch multiple instruments
        >>> instruments = [256265, 5633, 779521]
        >>> results = history.get_multiple_instruments_history(
        >>>     instruments=instruments,
        >>>     interval=Historical_Interval.min_15.value
        >>> )
    """

    BASE_URL = "https://kite.zerodha.com/oms/instruments/historical"
    RATE_LIMIT = 3  # requests per second (Kite API limit)
    RATE_LIMIT_WINDOW = 1.0  # seconds

    def __init__(
        self,
        enctoken: str = None,
        user_id: str = None,
        access_token_response: requests.Response = None,
    ):
        """
        Initialize KiteTickerHistory with authentication credentials.

        Args:
            enctoken: Authentication token obtained from KiteAuthenticator.get_enctoken()
            user_id: Zerodha user ID (e.g., 'AB1234')
            access_token_response: Response object from authentication containing cookies

        Raises:
            ValueError: If required authentication parameters are missing

        Note:
            If enctoken or user_id are not provided, the constructor will attempt to
            load them from environment variables or .env file using PKEnvironment.

        Example:
            >>> authenticator = KiteAuthenticator()
            >>> enctoken = authenticator.get_enctoken()
            >>>
            >>> history = KiteTickerHistory(
            >>>     enctoken=enctoken,
            >>>     user_id="AB1234",
            >>>     access_token_response=authenticator.access_token_response
            >>> )
        """
        from PKDevTools.classes.Environment import PKEnvironment

        local_secrets = PKEnvironment().allSecrets
        self.logger = default_logger()
        if enctoken is None or len(enctoken) == 0:
            enctoken = (
                os.environ.get(
                    "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
                ),
            )
        if user_id is None or len(user_id) == 0:
            user_id = os.environ.get(
                "KUSER", local_secrets.get("KUSER", "You need your Kite user")
            )
        self.enctoken = enctoken
        self.user_id = user_id
        self.session = requests.Session()
        self.last_request_time = 0
        self.lock = Lock()  # For thread-safe rate limiting
        self.failed_tokens = []

        self.update_session_headers()

        # Copy all cookies from the auth response
        # self.session.cookies.update(access_token_response.cookies)

        # Initialize database connection
        self.db_conn = libsql.connect(
            database=PKEnvironment().TDU, auth_token=PKEnvironment().TAT
        )

        # Only create if it doesn't exist
        if not self.table_exists(self.db_conn.cursor(), "instrument_history"):
            self._initialize_database()

    def update_session_headers(self):
        # Set all required headers and cookies
        self.session.headers.update(
            {
                "Authorization": f"enctoken {PKEnvironment().KTOKEN}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "X-Kite-Version": "3.0.0",
            }
        )

    def table_exists(self, cursor, table_name):
        cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        """,
            (table_name,),
        )
        return cursor.fetchone() is not None

    def _initialize_database(self):
        """
        Initialize the database schema for storing historical data.

        Creates the instrument_history table with appropriate columns and indexes
        for efficient data retrieval and querying.

        Table Structure:
            instrument_token: Unique instrument identifier
            timestamp: Candlestick timestamp
            open: Opening price
            high: Highest price
            low: Lowest price
            close: Closing price
            volume: Trading volume
            oi: Open interest (optional)
            interval: Time interval (day/minute/5minute/etc.)
            date: Generated date column for partitioning

        Indexes created for performance optimization on common query patterns.
        """
        create_table_query = """
        CREATE TABLE instrument_history (
            instrument_token INTEGER, -- NOT NULL,
            timestamp TEXT, -- NOT NULL,
            open REAL, -- NOT NULL,
            high REAL, -- NOT NULL,
            low REAL, -- NOT NULL,
            close REAL, -- NOT NULL,
            volume INTEGER, -- NOT NULL,
            oi INTEGER,
            interval TEXT, -- NOT NULL,
            date TEXT GENERATED ALWAYS AS ((substr(timestamp, 1, 10))) STORED,
            PRIMARY KEY (instrument_token, timestamp, interval)
        );
        """
        self.db_conn.execute(create_table_query)
        indices = [
            "CREATE INDEX idx_instrument_history_date ON instrument_history(date)",
            "CREATE INDEX idx_instrument_history_token_timestamp_interval_date ON instrument_history (instrument_token, timestamp, interval, date)",
            "CREATE INDEX idx_instrument_history_token ON instrument_history (instrument_token)",
            "CREATE INDEX idx_instrument_history_timestamp ON instrument_history (timestamp)",
            "CREATE INDEX idx_instrument_history_interval ON instrument_history (interval);",
        ]
        for index in indices:
            self.db_conn.execute(index)
        self.logger.debug("Database inititalised for instrument_history")

    def _rate_limit(self):
        """
        Enforce strict rate limiting according to Kite API limits.

        Implements token bucket algorithm to ensure maximum of 3 requests per second.
        This method blocks the calling thread if the rate limit would be exceeded.

        Note:
            Kite API allows maximum 3 requests per second. This method ensures
            compliance with this limit to avoid API bans or rate limit errors.
        """
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.RATE_LIMIT_WINDOW / self.RATE_LIMIT:
                delay = (self.RATE_LIMIT_WINDOW / self.RATE_LIMIT) - elapsed
                time.sleep(delay)
            self.last_request_time = time.time()

    def _format_date(self, date: Union[str, datetime]) -> str:
        """
        Convert date input to standardized YYYY-MM-DD format.

        Args:
            date: Date input as datetime object or string

        Returns:
            str: Formatted date string in YYYY-MM-DD format

        Example:
            >>> formatted = self._format_date(datetime(2023, 12, 25))
            >>> print(formatted)  # "2023-12-25"

            >>> formatted = self._format_date("2023-12-25")
            >>> print(formatted)  # "2023-12-25"
        """
        if isinstance(date, datetime):
            return date.strftime("%Y-%m-%d")
        return date

    def _save_to_database(self, instrument_token: int, data: Dict, interval: str):
        """
        Save historical candle data to the database in batch mode.

        Args:
            instrument_token: Unique instrument identifier
            data: Historical data dictionary containing 'candles' list
            interval: Time interval string (e.g., 'day', '5minute')

        Raises:
            Exception: If database operation fails, rolls back transaction

        Note:
            Uses batch insert with transaction for performance and data integrity.
            Implements ON CONFLICT IGNORE to handle duplicate data gracefully.

        Example:
            >>> data = {
            >>>     "candles": [
            >>>         ["2023-12-25 09:15:00", 100.0, 102.0, 99.5, 101.5, 10000, 5000],
            >>>         ["2023-12-25 09:16:00", 101.5, 103.0, 101.0, 102.5, 12000, 5500]
            >>>     ]
            >>> }
            >>> self._save_to_database(256265, data, "minute")
        """
        if not data or "candles" not in data or not data["candles"]:
            self.logger.warn(
                f"No candle data available for {instrument_token} for interval:{interval}"
            )
            return

        # Prepare batch insert with interval
        batch = []
        candles = data["candles"]
        for candle in candles:
            timestamp = candle[0]
            open_price = candle[1]
            high = candle[2]
            low = candle[3]
            close = candle[4]
            volume = candle[5] if len(candle) > 5 else None
            oi = candle[6] if len(candle) > 6 else None

            batch.append(
                (
                    instrument_token,
                    timestamp,
                    open_price,
                    high,
                    low,
                    close,
                    volume,
                    oi,
                    interval,  # Make sure interval is included
                )
            )

        # Use batch insert with ON CONFLICT IGNORE to avoid duplicates
        insert_query = """
        INSERT INTO instrument_history (
            instrument_token, timestamp, open, high, low, close, volume, oi,interval
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(instrument_token, timestamp, interval)
        DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            oi = excluded.oi
        """

        try:
            # Begin transaction explicitly
            self.db_conn.execute("BEGIN TRANSACTION")

            # Execute the batch insert
            self.db_conn.executemany(insert_query, batch)

            # Commit the transaction
            self.db_conn.execute("COMMIT")
            self.logger.info(
                f"Committed/inserted {len(candles)} rows for token:{instrument_token} and interval:{interval}"
            )

        except Exception as e:
            # Rollback if any error occurs
            self.db_conn.execute("ROLLBACK")
            print(f"Error saving to database: {str(e)}")
            self.logger.error(
                f"Rollback:Failed Inserting {len(candles)} rows for token:{instrument_token} and interval:{interval}\n{str(e)}"
            )
            self.failed_tokens.append(instrument_token)
            raise

    def _execute_safe(self, query, params, retrial=False):
        """
        Execute database query with error handling and automatic retry.

        Args:
            query: SQL query string
            params: Query parameters
            retrial: Internal flag for retry attempts (default: False)

        Returns:
            cursor: Database cursor with executed query

        Note:
            Automatically reconnects to database if connection issues are detected.
        """
        try:
            self.logger.debug(f"Executing:Retrial:{retrial} for query:{query}")
            cursor = self.db_conn.cursor()
            cursor.execute(
                query,
                params,
            )
        except ValueError as e:
            self.logger.error(
                f"Error executing:Retrial:{retrial} for query:{query}. {e}"
            )
            if not retrial:
                # Re-Initialize database connection
                self.db_conn = libsql.connect(
                    database=PKEnvironment().TDU, auth_token=PKEnvironment().TAT
                )
                return self._execute_safe(query=query, params=params, retrial=True)
        return cursor

    def timedelta_for_interval(self, interval: str = "day"):
        intervals_dict = {
            "day": timedelta(days=MAX_CANDLES_COUNT),
            "minute": timedelta(minutes=MAX_CANDLES_COUNT),
            "5minute": timedelta(minutes=5 * MAX_CANDLES_COUNT),
            "10minute": timedelta(minutes=10 * MAX_CANDLES_COUNT),
            "30minute": timedelta(minutes=30 * MAX_CANDLES_COUNT),
            "60minute": timedelta(minutes=60 * MAX_CANDLES_COUNT),
        }
        return intervals_dict.get(interval, timedelta(days=MAX_CANDLES_COUNT))

    def get_historical_data(
        self,
        instrument_token: int,
        from_date: Union[str, datetime] = None,
        to_date: Union[str, datetime] = None,
        interval: str = "day",
        oi: bool = True,
        continuous: bool = False,
        max_retries: int = 3,
        forceFetch=False,
        insertOnly=False,
    ) -> Dict:
        """
        Fetch historical data for a single instrument with intelligent caching.

        This method first checks the local database for existing data and only
        fetches from the Kite API if necessary (fresh data needed or not in database).

        Args:
            instrument_token: Zerodha instrument token (required)
            from_date: Start date (YYYY-MM-DD or datetime, defaults to 365 days ago)
            to_date: End date (YYYY-MM-DD or datetime, defaults to current date)
            interval: Time interval (default: "day", see Historical_Interval enum)
            oi: Include open interest data (default: True)
            continuous: For continuous contracts (default: False)
            max_retries: Maximum API retry attempts (default: 3)
            forceFetch: Bypass cache and force API fetch (default: False)
            insertOnly: Only insert new data without returning (default: False)

        Returns:
            Dict: Historical data with candles and metadata
            Format: {
                "status": "success",
                "data": {
                    "candles": [
                        [timestamp, open, high, low, close, volume, oi],
                        ...
                    ],
                    "source": "database" or "api"
                }
            }

        Raises:
            ValueError: If instrument_token is missing or invalid
            requests.exceptions.RequestException: If API calls fail after retries

        Example:
            >>> # Fetch daily data for Nifty 50
            >>> data = history.get_historical_data(
            >>>     instrument_token=256265,
            >>>     from_date="2023-01-01",
            >>>     to_date="2023-12-31",
            >>>     interval=Historical_Interval.day.value
            >>> )
            >>>
            >>> # Fetch intraday 5-minute data
            >>> data = history.get_historical_data(
            >>>     instrument_token=256265,
            >>>     interval=Historical_Interval.min_5.value,
            >>>     forceFetch=True  # Bypass cache
            >>> )
        """
        if instrument_token is None or len(str(instrument_token)) == 0:
            raise ValueError("instrument_token is required")
        if from_date is None or len(from_date) == 0:
            from_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime()
                - self.timedelta_for_interval(interval=interval.lower())
            )
        if to_date is None or len(to_date) == 0:
            to_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime()
            )

        formatted_from_date = self._format_date(from_date)
        formatted_to_date = self._format_date(to_date)
        current_date = PKDateUtilities.YmdStringFromDate(
            PKDateUtilities.currentDateTime()
        )

        # Check if we need fresh data (for current day during market hours)
        need_fresh_data = (
            formatted_to_date >= current_date and self._is_market_open()
        ) or forceFetch

        if not need_fresh_data and not insertOnly:
            # Try to get data from database first
            select_query = """
            SELECT timestamp, open, high, low, close, volume, oi
            FROM instrument_history
            WHERE instrument_token = ?
            AND interval = ?
            AND date BETWEEN ? AND ?
            ORDER BY timestamp;
            """

            cursor = self._execute_safe(
                select_query,
                (instrument_token, interval, formatted_from_date, formatted_to_date),
            )
            rows = cursor.fetchall()
            self.logger.debug(f"Fetched {len(rows)} rows from the database")
            if rows:
                candles = []
                for row in rows:
                    candle = [
                        row[0],  # timestamp
                        float(row[1]),  # open
                        float(row[2]),  # high
                        float(row[3]),  # low
                        float(row[4]),  # close
                        int(row[5]) if row[5] is not None else 0,  # volume
                        int(row[6]) if row[6] is not None else 0,  # oi
                    ]
                    candles.append(candle)

                return {
                    "status": "success",
                    "data": {"candles": candles, "source": "database"},
                }

        if insertOnly:
            # Try to get what was the last saved date
            select_query = """
            SELECT count(1) as total_count, max(date) as max_date
            FROM instrument_history
            WHERE instrument_token = ?
            AND interval = ?
            AND date BETWEEN ? AND ?
            """

            cursor = self._execute_safe(
                select_query,
                (instrument_token, interval, formatted_from_date, formatted_to_date),
            )
            max_date = formatted_from_date
            rows = cursor.fetchall()
            self.logger.debug(f"Fetched {len(rows)} rows from the database")
            for row in rows:
                rows_count = row[0]
                max_date = (
                    formatted_from_date
                    if (rows_count == 0 or row[1] is None)
                    else row[1]
                )
                break

        # If we need fresh data or data not found in database, fetch from API
        params = {
            "user_id": self.user_id,
            "oi": "1" if oi else "0",
            "from": max_date,
            "to": formatted_to_date,
            "continuous": "1" if continuous else "0",
        }

        # if rows_count >= 249 and formatted_to_date != current_date:
        url = f"{self.BASE_URL}/{instrument_token}/{interval}"
        last_error = None
        response = None
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                self.logger.debug(
                    f"Fetching history data from {url}?{'&'.join([f'{key}={value}' for key, value in params.items()])}"
                )
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()["data"]

                # Save to database if we got valid candles
                if data.get("candles"):
                    self._save_to_database(
                        instrument_token=instrument_token, data=data, interval=interval
                    )

                data["source"] = "api"
                return data
            except requests.exceptions.RequestException as e:
                self.logger.error(e)
                last_error = e
                if attempt < max_retries - 1:
                    if response and response.status_code not in [400, 500]:
                        if response.status_code in [401, 403]:
                            if attempt <= max_retries - 2:
                                # Still the first try. Let's just retry with a possible existing
                                # valid token that bot may already have
                                from pkbrokers.kite.examples.pkkite import (
                                    remote_bot_auth_token,
                                )

                                remote_bot_auth_token()
                            elif attempt <= max_retries - 1:
                                # We failed even with the most recent token that was provided by
                                # the bot. So let's try and refresh the token instead and ask
                                # the bot to refresh the token.
                                self.logger.error(
                                    "❌❌❌ There may be a need for refreshing the token! ❌❌❌"
                                )
                                from pkbrokers.kite.examples.pkkite import (
                                    try_refresh_token,
                                )

                                try_refresh_token()
                        self.update_session_headers()
                    time.sleep(2**attempt)
                else:
                    if response and response.status_code in [401, 403]:
                        self.logger.error(
                            "❌❌❌ Either check the rate-limit violations or manually refresh the token! ❌❌❌"
                        )
                continue

        self.logger.error(
            f"Failed after {max_retries} attempts for {instrument_token}: {str(last_error)}"
        )
        raise requests.exceptions.RequestException(
            f"Failed after {max_retries} attempts for {instrument_token}: {str(last_error)}"
        )

    def get_multiple_instruments_history(
        self,
        instruments: List[int],
        from_date: Union[str, datetime] = None,
        to_date: Union[str, datetime] = None,
        interval: str = "day",
        oi: bool = True,
        batch_size: int = 3,
        max_retries: int = 2,
        delay: float = 1.0,
        forceFetch=False,
        insertOnly=False,
        past_offset=0,
    ) -> Dict[int, Dict]:
        """
        Fetch historical data for multiple instruments with optimized batching.

        This method efficiently processes multiple instruments by:
        1. First checking local database for existing data
        2. Only fetching missing or fresh data from API
        3. Using batch processing with rate limiting
        4. Providing progress logging

        Args:
            instruments: List of instrument tokens (required)
            from_date: Start date (defaults to 365 days ago)
            to_date: End date (defaults to current date)
            interval: Time interval (default: "day")
            oi: Include open interest (default: True)
            batch_size: Number of instruments per batch (default: 3)
            max_retries: Maximum retry attempts per instrument (default: 2)
            delay: Delay between batches in seconds (default: 1.0)
            forceFetch: Force API fetch bypassing cache (default: False)
            insertOnly: Only insert data without returning (default: False)
            past_offset: The number of days in the past for which data needs to be fetched.

        Returns:
            Dict[int, Dict]: Dictionary mapping instrument tokens to their historical data
            Format: {
                256265: {"status": "success", "data": {...}},
                5633: {"status": "success", "data": {...}},
                ...
            }

        Raises:
            ValueError: If instruments list is empty

        Example:
            >>> # Fetch data for multiple Nifty stocks
            >>> nifty_stocks = [256265, 5633, 779521, 1270529]
            >>> results = history.get_multiple_instruments_history(
            >>>     instruments=nifty_stocks,
            >>>     interval=Historical_Interval.min_15.value,
            >>>     from_date="2023-12-20",
            >>>     to_date="2023-12-25"
            >>> )
            >>>
            >>> # Process results
            >>> for token, data in results.items():
            >>>     if data["status"] == "success":
            >>>         print(f"Token {token}: {len(data['data']['candles'])} candles")
        """
        begin_time = time.time()
        if not instruments:
            raise ValueError("list of instruments is required")
        if from_date is None or len(from_date) == 0:
            from_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime()
                - self.timedelta_for_interval(interval=interval.lower())
                - timedelta(days=past_offset)
            )
        if to_date is None or len(to_date) == 0:
            to_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime()
            )
        formatted_from_date = self._format_date(from_date)
        formatted_to_date = self._format_date(to_date)
        current_date = PKDateUtilities.YmdStringFromDate(
            PKDateUtilities.currentDateTime()
        )
        is_market_open = self._is_market_open()

        results = {}
        batch_size = min(batch_size, self.RATE_LIMIT)

        # Determine which instruments need fresh data
        need_fresh_data = (
            formatted_to_date >= current_date and is_market_open
        ) or forceFetch

        self.logger.debug(
            f"Fresh data needed:{need_fresh_data}. Fetching instrument history with batch_size:{batch_size} at: {begin_time} from: {formatted_from_date} to:{formatted_to_date}. Market open:{is_market_open}"
        )

        if not need_fresh_data and not insertOnly:
            # Try to get all possible data from database first
            placeholders = ",".join(["?"] * len(instruments))
            select_query = f"""
            SELECT instrument_token, timestamp, open, high, low, close, volume, oi
            FROM instrument_history
            WHERE instrument_token IN ({placeholders})
            AND interval = ?
            AND date BETWEEN ? AND ?
            -- ORDER BY instrument_token, timestamp;
            """

            cursor = cursor = self._execute_safe(
                select_query,
                (*instruments, interval, formatted_from_date, formatted_to_date),
            )
            # Group results by instrument_token
            db_data = {}
            current_instrument = None
            candles = []
            rows = cursor.fetchall()
            self.logger.debug(f"Fetched {len(rows)} rows from the database.")
            for row in rows:
                if row[0] != current_instrument:
                    if current_instrument is not None:
                        db_data[current_instrument] = {
                            "status": "success",
                            "data": {"candles": candles.copy(), "source": "database"},
                        }
                    current_instrument = row[0]
                    candles = []

                candle = [
                    row[1],  # timestamp
                    float(row[2]),  # open
                    float(row[3]),  # high
                    float(row[4]),  # low
                    float(row[5]),  # close
                    int(row[6]) if row[6] is not None else 0,  # volume
                    int(row[7]) if row[7] is not None else 0,  # oi
                ]
                candles.append(candle)

            if current_instrument is not None:
                db_data[current_instrument] = {
                    "status": "success",
                    "data": {"candles": candles, "source": "database"},
                }

            results.update(db_data)
            # Only fetch instruments that weren't found in database
            instruments_to_fetch = [i for i in instruments if i not in db_data]
        else:
            # Need fresh data for all instruments
            instruments_to_fetch = instruments

        # Process instruments that need API fetch
        counter = 0
        batch_begin = time.time()
        self.logger.info(
            f"Going to fetch historical data for interval:{interval} for dates from: {from_date} to:{to_date}"
        )
        for i in range(0, len(instruments_to_fetch), batch_size):
            batch = instruments_to_fetch[i : i + batch_size]
            for instrument in batch:
                try:
                    batch_begin = time.time()
                    api_data = self.get_historical_data(
                        instrument_token=instrument,
                        from_date=from_date,
                        to_date=to_date,
                        interval=interval,
                        oi=oi,
                        max_retries=max_retries,
                        forceFetch=forceFetch,
                        insertOnly=insertOnly,
                    )
                    results[instrument] = api_data
                    counter = counter + 1
                except Exception as e:
                    results[instrument] = {
                        "status": "failed",
                        "error": str(e),
                    }
                self.logger.info(
                    f"Fetched/Saved {counter} of {len(instruments_to_fetch)} tokens in {'%.3f' % (time.time() - begin_time)} sec."
                )
            requiredDelay = delay - (time.time() - batch_begin)
            if i + batch_size < len(instruments_to_fetch) and requiredDelay >= 0:
                time.sleep(requiredDelay)

        return results

    def __del__(self):
        """
        Cleanup method to close database connection when object is destroyed.

        Ensures proper resource cleanup to prevent database connection leaks.
        """
        if hasattr(self, "db_conn"):
            try:
                self.db_conn.close()
            except BaseException:
                pass

    def _is_market_open(self) -> bool:
        """
        Check if the stock market is currently open for trading.

        Returns:
            bool: True if market is open, False otherwise

        Note:
            Uses PKDateUtilities to determine market hours based on Indian stock market
            timing (9:15 AM to 3:30 PM IST on trading days).
        """
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities

        current_date = PKDateUtilities.YmdStringFromDate(
            PKDateUtilities.currentDateTime()
        )
        tradingDate = PKDateUtilities.YmdStringFromDate(PKDateUtilities.tradingDate())
        return current_date == tradingDate
