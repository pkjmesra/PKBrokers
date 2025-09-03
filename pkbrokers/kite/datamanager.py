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
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import libsql
import pandas as pd
import requests
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes import Archiver

class InstrumentDataManager:
    """
    A comprehensive data manager for financial instrument data synchronization and retrieval.

    This class handles data from multiple sources including GitHub-hosted pickle files,
    remote databases, and Kite API. It provides seamless data synchronization, updating,
    and retrieval for financial analysis and screening.

    Attributes:
        pickle_url (str): GitHub repository URL for the pickle file
        raw_pickle_url (str): Raw content URL for the pickle file
        db_conn: Database connection object
        pickle_data (Dict): Loaded pickle data
        logger: Logger instance for debugging and information

    Example:
        >>> from pkbrokers.kite.datamanager import InstrumentDataManager
        >>> manager = InstrumentDataManager()
        >>> success = manager.execute()
        >>> if success:
        >>>     data = manager.get_data_for_symbol("RELIANCE")
        >>>     print(f"Reliance data: {data}")
    """

    def __init__(self):
        """
        Initialize the InstrumentDataManager with default URLs and empty data storage.

        The manager is configured to work with PKScreener's GitHub repository structure
        and requires proper environment variables for database connections.
        """
        exists, path = Archiver.afterMarketStockDataExists()
        self.pickle_file_name = path
        self.pickle_exists = exists
        self.local_pickle_path = os.path.join(Archiver.get_user_data_dir(), self.pickle_file_name)
        self.pickle_url = f"https://github.com/pkjmesra/PKScreener/tree/actions-data-download/results/Data/{path}"
        self.raw_pickle_url = f"https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/results/Data/{path}"
        self.db_conn = None
        self.pickle_data = None
        self.logger = default_logger()

    def _connect_to_database(self) -> bool:
        """
        Establish connection to remote Turso database using libsql.

        Uses environment variables for database URL and authentication token.
        Required environment variables:
        - TDU: Turso Database URL
        - TAT: Turso Authentication Token

        Returns:
            bool: True if connection successful, False otherwise

        Example:
            >>> manager = InstrumentDataManager()
            >>> connected = manager._connect_to_database()
            >>> if connected:
            >>>     print("Database connection established")
        """
        try:
            self.db_conn = libsql.connect(
                database=PKEnvironment().TDU, auth_token=PKEnvironment().TAT
            )
            return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return False

    def _check_pickle_exists(self) -> bool:
        """
        Check if the pickle file exists on GitHub repository.

        Uses HTTP HEAD request to verify file existence without downloading content.

        Returns:
            bool: True if file exists (HTTP 200), False otherwise

        Example:
            >>> exists = manager._check_pickle_exists()
            >>> if exists:
            >>>     print("Pickle file available on GitHub")
        """
        try:
            response = requests.head(self.raw_pickle_url)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def _load_pickle_from_github(self) -> Optional[Dict]:
        """
        Download and load pickle data from GitHub raw content URL.

        Returns:
            Optional[Dict]: Loaded pickle data dictionary if successful, None otherwise

        Raises:
            requests.HTTPError: If download fails
            pickle.UnpicklingError: If file content is not valid pickle data

        Example:
            >>> data = manager._load_pickle_from_github()
            >>> if data:
            >>>     print(f"Loaded {len(data)} instruments")
        """
        try:
            response = requests.get(self.raw_pickle_url)
            response.raise_for_status()
            self.pickle_data = pickle.loads(response.content)
            return self.pickle_data
        except Exception as e:
            self.logger.error(f"Failed to load pickle from GitHub: {e}")
            return None

    def _get_recent_data_from_kite(self) -> Optional[Dict]:
        """
        Fetch recent 2-3 days of market data using Kite API.

        This method:
        1. Retrieves trading symbols from available sources
        2. Fetches historical data for past 3 days
        3. Optionally saves data to database
        4. Returns structured market data

        Returns:
            Optional[Dict]: Recent market data dictionary if successful, None otherwise

        Example:
            >>> recent_data = manager._get_recent_data_from_kite()
            >>> if recent_data:
            >>>     print(f"Got {len(recent_data)} recent data points")
        """
        try:
            from pkbrokers.kite.instrumentHistory import KiteTickerHistory

            kite_history = KiteTickerHistory()

            # Get tradingsymbols from pickle or database
            tradingsymbols = self._get_tradingsymbols()

            if not tradingsymbols:
                self.logger.debug("No tradingsymbols found to fetch data")
                return None

            # Get data for past 3 days
            end_date = datetime.now()
            start_date = self._format_date(end_date - timedelta(days=3))
            end_date = self._format_date(end_date)
            # Fetch historical data
            historical_data = kite_history.get_multiple_instruments_history(
                tradingsymbols=tradingsymbols, from_date=start_date, to_date=end_date
            )

            # Save to database
            if hasattr(kite_history, "_save_to_database"):
                kite_history._save_to_database(historical_data, "instrument_history")

            return historical_data

        except ImportError:
            self.logger.debug("KiteTickerHistory module not available")
            return None
        except Exception as e:
            self.logger.debug(f"Error fetching data from Kite: {e}")
            return None

    def _format_date(self, date: Union[str, datetime]) -> str:
        """
        Convert date object or string to standardized YYYY-MM-DD format.

        Args:
            date: Date input as datetime object or string

        Returns:
            str: Formatted date string in YYYY-MM-DD format

        Example:
            >>> formatted = manager._format_date(datetime(2023, 12, 25))
            >>> print(formatted)  # "2023-12-25"
        """
        if isinstance(date, datetime):
            return date.strftime("%Y-%m-%d")
        return date

    def _get_tradingsymbols(self) -> List[str]:
        """
        Retrieve list of trading symbols from available data sources.

        Priority:
        1. Existing pickle data (if loaded)
        2. Database (if connected)

        Returns:
            List[str]: List of trading symbols

        Example:
            >>> symbols = manager._get_tradingsymbols()
            >>> print(f"Found {len(symbols)} trading symbols")
        """
        if self.pickle_data:
            # Extract tradingsymbols from pickle data
            return list(self.pickle_data.keys())
        else:
            # Fetch from database
            return self._get_tradingsymbols_from_db()

    def _get_tradingsymbols_from_db(self) -> List[str]:
        """
        Fetch distinct trading symbols from instruments database table.

        Returns:
            List[str]: List of unique trading symbols from database

        Example:
            >>> symbols = manager._get_tradingsymbols_from_db()
            >>> print(f"Database has {len(symbols)} symbols")
        """
        if not self._connect_to_database():
            return []

        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT DISTINCT tradingsymbol FROM instruments")
            results = cursor.fetchall()
            return [row[0] for row in results] if results else []
        except Exception as e:
            self.logger.debug(f"Error fetching tradingsymbols from database: {e}")
            return []

    def _fetch_data_from_database(self) -> Dict:
        """
        Fetch 365 days of historical data from instrument_history table.

        Returns:
            Dict: Structured historical data with trading symbols as keys

        Example:
            >>> historical_data = manager._fetch_data_from_database()
            >>> print(f"Fetched {len(historical_data)} symbols from database")
        """
        if not self._connect_to_database():
            return {}

        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = self._format_date(end_date - timedelta(days=365))
            end_date = self._format_date(end_date)
            # Fetch instrument history data
            cursor = self.db_conn.cursor()
            query = """
                SELECT ih.*, i.tradingsymbol
                FROM instrument_history ih
                JOIN instruments i ON ih.instrument_token = i.instrument_token
                WHERE ih.timestamp >= ? AND ih.timestamp <= ?
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()

            # Fetch column names
            columns = [desc[0] for desc in cursor.description]

            return self._process_database_data(results, columns)

        except Exception as e:
            self.logger.debug(f"Error fetching data from database: {e}")
            return {}

    def _process_database_data(self, results: List, columns: List[str]) -> Dict:
        """
        Process raw database results into structured dictionary format.

        Args:
            results: Raw database query results
            columns: Column names from database query

        Returns:
            Dict: Processed data with trading symbols as keys and date-based data as values

        Example:
            >>> processed = manager._process_database_data(results, columns)
        """
        master_data = {}

        # Convert to DataFrame for easier processing
        df = pd.DataFrame(results, columns=columns)

        if df.empty:
            return master_data

        # Group by tradingsymbol and process
        for tradingsymbol, group in df.groupby("tradingsymbol"):
            # Convert to dictionary format with date as key
            symbol_data = {}
            for _, row in group.iterrows():
                date_key = (
                    row["timestamp"].date()
                    if hasattr(row["timestamp"], "date")
                    else row["timestamp"]
                )
                symbol_data[date_key] = {
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "volume": row.get("volume"),
                    "oi": row.get("oi"),
                    "instrument_token": row.get("instrument_token"),
                }

            master_data[tradingsymbol] = symbol_data

        return master_data

    def _update_pickle_file(self, new_data: Dict):
        """
        Update local pickle file with new data, merging with existing data.

        Args:
            new_data: Dictionary containing new data to merge

        Example:
            >>> manager._update_pickle_file(new_data)
            >>> print("Pickle file updated successfully")
        """
        if self.pickle_data:
            # Merge new data with existing pickle data
            for tradingsymbol, daily_data in new_data.items():
                if tradingsymbol in self.pickle_data:
                    # Update existing symbol data
                    self.pickle_data[tradingsymbol].update(daily_data)
                else:
                    # Add new symbol
                    self.pickle_data[tradingsymbol] = daily_data
        else:
            # Create new pickle data
            self.pickle_data = new_data

        # Save to local pickle file
        with open(self.local_pickle_path, "wb") as f:
            pickle.dump(self.pickle_data, f)

        self.logger.debug("Pickle file updated successfully")

    def get_data_for_symbol(self, tradingsymbol: str) -> Optional[Dict]:
        """
        Retrieve full year's data for a specific trading symbol.

        Args:
            tradingsymbol: Trading symbol to retrieve data for (e.g., "RELIANCE")

        Returns:
            Optional[Dict]: Data for the specified symbol if available, None otherwise

        Example:
            >>> reliance_data = manager.get_data_for_symbol("RELIANCE")
            >>> if reliance_data:
            >>>     print(f"Reliance has {len(reliance_data)} days of data")
        """
        if self.pickle_data and tradingsymbol in self.pickle_data:
            return self.pickle_data[tradingsymbol]
        return None

    def execute(self) -> bool:
        """
        Main execution method that orchestrates the data synchronization process.

        Workflow:
        1. Check if pickle file exists on GitHub
        2. If exists: download and load, then fetch recent data from Kite
        3. If not exists: fetch full year data from database
        4. Update local pickle file with latest data

        Returns:
            bool: True if data was successfully loaded/created, False otherwise

        Example:
            >>> manager = InstrumentDataManager()
            >>> success = manager.execute()
            >>> if success:
            >>>     print("Data synchronization completed successfully")
        """
        self.logger.debug("Checking for existing pickle file...")

        if self._check_pickle_exists():
            self.logger.debug("Pickle file found on GitHub")
            self._load_pickle_from_github()

            # Get recent data and update
            recent_data = self._get_recent_data_from_kite()
            if recent_data:
                self._update_pickle_file(recent_data)

        else:
            self.logger.debug("Pickle file not found, fetching from database...")
            # Fetch data from database
            historical_data = self._fetch_data_from_database()

            if historical_data:
                self.pickle_data = historical_data
                # Save to local pickle file
                with open(self.local_pickle_path, "wb") as f:
                    pickle.dump(self.pickle_data, f)
                self.logger.debug("Pickle file created from database data")
            else:
                self.logger.debug("No data available from database")

        return self.pickle_data is not None
