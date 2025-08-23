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


class Historical_Interval(Enum):
    day = "day"
    min_1 = "minute"
    min_5 = "5minute"
    min_10 = "10minute"
    min_15 = "15minute"
    min_30 = "30minute"
    min_60 = "60minute"


class KiteTickerHistory:
    """
    Fetches historical data from Zerodha's Kite Connect API with:
    - Proper cookie handling from access_token_response
    - Strict rate limiting (3 requests/second)
    - Batch processing with automatic retries
    - SQLite database integration for caching

    Usage:
        authenticator = KiteAuthenticator()
        enctoken = authenticator.get_enctoken(...)

        history = KiteTickerHistory(
            enctoken=enctoken,
            user_id="YourUserId",
            auth_cookies=authenticator.access_token_response.headers.get('Set-Cookie', '')
        )
    """

    BASE_URL = "https://kite.zerodha.com/oms/instruments/historical"
    RATE_LIMIT = 3  # requests per second
    RATE_LIMIT_WINDOW = 1.0  # seconds

    def __init__(
        self,
        enctoken: str = None,
        user_id: str = None,
        access_token_response: requests.Response = None,
    ):
        """
        Initialize with authentication token and cookies

        Args:
            enctoken: Authentication token from KiteAuthenticator
            user_id: Zerodha user ID (e.g., 'YourUserId')
            access_token_response: Cookies/headers from access_token_response (along with Set-Cookie headers)
        """
        from dotenv import dotenv_values

        local_secrets = dotenv_values(".env.dev")
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

        # Set all required headers and cookies
        self.session.headers.update(
            {
                "Authorization": f"enctoken {self.enctoken}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "X-Kite-Version": "3.0.0",
            }
        )

        # Copy all cookies from the auth response
        self.session.cookies.update(access_token_response.cookies)

        # Initialize database connection
        self.db_conn = libsql.connect(
            database=PKEnvironment().TDU, auth_token=PKEnvironment().TAT
        )

        # Create table if not exists
        self._initialize_database()

    def _initialize_database(self):
        """Create the instrument_history table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS instrument_history (
            instrument_token INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            oi INTEGER,
            interval TEXT NOT NULL,
            date TEXT GENERATED ALWAYS AS ((substr(timestamp, 1, 10))) STORED,
            PRIMARY KEY (instrument_token, timestamp, interval)
        );
        """
        self.db_conn.execute(create_table_query)
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_instrument_history_date ON instrument_history(date)",
            "CREATE INDEX IF NOT EXISTS idx_instrument_history_token_timestamp_interval_date ON instrument_history (instrument_token, timestamp, interval, date)",
            "CREATE INDEX IF NOT EXISTS idx_instrument_history_token ON instrument_history (instrument_token)",
            "CREATE INDEX IF NOT EXISTS idx_instrument_history_timestamp ON instrument_history (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_instrument_history_interval ON instrument_history (interval);",
        ]
        for index in indices:
            self.db_conn.execute(index)
        self.logger.info("Database inititalised for instrument_history")

    def _rate_limit(self):
        """Enforce strict rate limiting (3 requests/second)"""
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.RATE_LIMIT_WINDOW / self.RATE_LIMIT:
                delay = (self.RATE_LIMIT_WINDOW / self.RATE_LIMIT) - elapsed
                time.sleep(delay)
            self.last_request_time = time.time()

    def _format_date(self, date: Union[str, datetime]) -> str:
        """Convert date to YYYY-MM-DD format"""
        if isinstance(date, datetime):
            return date.strftime("%Y-%m-%d")
        return date

    def _save_to_database(self, instrument_token: int, data: Dict, interval: str):
        """
        Save historical data to SQLite database in batch

        Args:
            instrument_token: The instrument token
            data: Historical data from Kite API
            interval: The time interval (day, minute, etc.)
            from_date: Start date of the query
            to_date: End date of the query
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
        INSERT OR IGNORE INTO instrument_history (
            instrument_token, timestamp, open, high, low, close, volume, oi,
            interval
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
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
        try:
            self.logger.info(f"Executing:Retrial:{retrial} for query:{query}")
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
        Fetch historical data for an instrument with proper authentication

        Args:
            instrument_token: Zerodha instrument token
            from_date: Start date (YYYY-MM-DD or datetime)
            to_date: End date (YYYY-MM-DD or datetime)
            interval: Time interval (minute/day/3minute/etc.)
            oi: Include open interest data
            continuous: For continuous contracts
            max_retries: Maximum retry attempts

        Returns:
            Dictionary with historical data in Kite format
        """
        if instrument_token is None or len(str(instrument_token)) == 0:
            raise ValueError("instrument_token is required")
        if from_date is None or len(from_date) == 0:
            from_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime() - timedelta(days=365)
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
            self.logger.info(f"Fetched {len(rows)} rows from the database")
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
            self.logger.info(f"Fetched {len(rows)} rows from the database")
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

        for attempt in range(max_retries):
            try:
                self._rate_limit()
                self.logger.info(
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
                    time.sleep(2**attempt)
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
    ) -> Dict[int, Dict]:
        """
        Fetch historical data for multiple instruments with rate limiting

        Args:
            instruments: List of instrument tokens
            from_date: Start date
            to_date: End date
            interval: Time interval
            oi: Include open interest
            batch_size: Requests per batch
            max_retries: Retry attempts
            delay: Delay between batches

        Returns:
            Dictionary mapping instrument tokens to their historical data
        """
        begin_time = time.time()
        if not instruments:
            raise ValueError("list of instruments is required")
        if from_date is None or len(from_date) == 0:
            from_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime() - timedelta(days=365)
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
            self.logger.info(f"Fetched {len(rows)} rows from the database.")
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
        """Clean up database connection when object is destroyed"""
        if hasattr(self, "db_conn"):
            try:
                self.db_conn.close()
            except BaseException:
                pass

    def _is_market_open(self) -> bool:
        """Check if market is currently open"""
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities

        current_date = PKDateUtilities.YmdStringFromDate(
            PKDateUtilities.currentDateTime()
        )
        tradingDate = PKDateUtilities.YmdStringFromDate(PKDateUtilities.tradingDate())
        return current_date == tradingDate


"""
# First authenticate
from pkbrokers.kite.authenticator import KiteAuthenticator
authenticator = KiteAuthenticator()
enctoken = authenticator.get_enctoken(...)  # Your credentials

# Create history client with the full response object
history = KiteTickerHistory(
    enctoken=enctoken,
    user_id="whatever",
    access_token_response=authenticator.access_token_response
)

# Single request (automatically rate limited)
data = history.get_historical_data(
    instrument_token=1793,
    from_date="2024-08-10",
    to_date="2025-08-11",
    interval="day"
)

# Batch processing (automatically respects 3req/sec limit)
batch_data = history.get_multiple_instruments(
    instruments=[256265, 260105, 1793, 11536],
    from_date="2024-01-01",
    to_date="2024-01-31",
    interval="5minute"
)
"""
