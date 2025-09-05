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
import pickle
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
from pathlib import Path

import libsql
import pandas as pd
import requests
import pytz
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes import Archiver
from PKDevTools.classes.PKDateUtilities import PKDateUtilities
from pkbrokers.kite.threadSafeDatabase import DEFAULT_DB_PATH

class InstrumentDataManager:
    """
    A comprehensive data manager for financial instrument data synchronization and retrieval.

    This class handles data from multiple sources including local/remote pickle files,
    remote databases (Turso/SQLite), Kite API, and ticks.json files. It provides seamless
    data synchronization, updating, and retrieval for financial analysis and screening.

    Key Features:
    - Local-first approach: Checks for pickle file in user data directory first
    - Incremental updates: Fetches only missing data from the latest available date
    - Multi-source integration: Supports Turso DB, SQLite, Kite API, and ticks.json
    - Automated synchronization: Orchestrates complete data update pipeline

    Attributes:
        pickle_url (str): GitHub repository URL for the pickle file
        raw_pickle_url (str): Raw content URL for the pickle file
        db_conn: Database connection object
        pickle_data (Dict): Loaded pickle data
        logger: Logger instance for debugging and information
        local_pickle_path (Path): Local path to pickle file in user data directory
        ticks_json_path (Path): Local path to ticks.json file

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
        and requires proper environment variables for database connections. It sets up
        local file paths using the user data directory.
        """
        exists, path = Archiver.afterMarketStockDataExists(date_suffix=False)
        self.pickle_file_name = path
        self.pickle_exists = exists
        self.local_pickle_path = Path(Archiver.get_user_data_dir()) / self.pickle_file_name
        self.ticks_json_path = Path(Archiver.get_user_data_dir()) / "ticks.json"
        self.pickle_url = f"https://github.com/pkjmesra/PKScreener/tree/actions-data-download/results/Data/{path}"
        self.raw_pickle_url = f"https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/results/Data/{path}"
        self.db_conn = None
        self.pickle_data = None
        self.db_type = "turso" or PKEnvironment().DB_TYPE
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
            if self.db_type == "turso":
                self.db_conn = self._create_turso_connection()
            else:
                self.db_conn = self._create_local_connection()
            return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return False

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
            conn.execute("PRAGMA cache_size=-2000")
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

            url = PKEnvironment().TDU
            auth_token = PKEnvironment().TAT

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

    def _check_pickle_exists_locally(self) -> bool:
        """
        Check if the pickle file exists in the local user data directory.

        Returns:
            bool: True if file exists locally, False otherwise

        Example:
            >>> exists = manager._check_pickle_exists_locally()
            >>> if exists:
            >>>     print("Pickle file available locally")
        """
        return self.local_pickle_path.exists() and self.local_pickle_path.stat().st_size > 0

    def _check_pickle_exists_remote(self) -> bool:
        """
        Check if the pickle file exists on GitHub repository.

        Uses HTTP HEAD request to verify file existence without downloading content.
        Only called if local file doesn't exist.

        Returns:
            bool: True if file exists (HTTP 200), False otherwise

        Example:
            >>> exists = manager._check_pickle_exists_remote()
            >>> if exists:
            >>>     print("Pickle file available on GitHub")
        """
        try:
            response = requests.head(self.raw_pickle_url)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def _load_pickle_from_local(self) -> Optional[Dict]:
        """
        Load pickle data from local user data directory.

        Returns:
            Optional[Dict]: Loaded pickle data dictionary if successful, None otherwise

        Raises:
            pickle.UnpicklingError: If file content is not valid pickle data
            IOError: If file cannot be read

        Example:
            >>> data = manager._load_pickle_from_local()
            >>> if data:
            >>>     print(f"Loaded {len(data)} instruments from local file")
        """
        try:
            with open(self.local_pickle_path, 'rb') as f:
                self.pickle_data = pickle.load(f)
            self.logger.debug(f"Loaded pickle data from local file: {self.local_pickle_path}")
            return self.pickle_data
        except Exception as e:
            self.logger.error(f"Failed to load local pickle file: {e}")
            return None

    def _load_pickle_from_github(self) -> Optional[Dict]:
        """
        Download and load pickle data from GitHub raw content URL.
        Only called if local file doesn't exist.

        Returns:
            Optional[Dict]: Loaded pickle data dictionary if successful, None otherwise

        Raises:
            requests.HTTPError: If download fails
            pickle.UnpicklingError: If file content is not valid pickle data

        Example:
            >>> data = manager._load_pickle_from_github()
            >>> if data:
            >>>     print(f"Loaded {len(data)} instruments from GitHub")
        """
        try:
            response = requests.get(self.raw_pickle_url)
            response.raise_for_status()
            
            # Ensure directory exists
            self.local_pickle_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to local file first
            with open(self.local_pickle_path, 'wb') as f:
                f.write(response.content)
            
            # Then load from local file
            self.pickle_data = pickle.loads(response.content)
            self.logger.debug(f"Downloaded and loaded pickle data from GitHub: {self.raw_pickle_url}")
            return self.pickle_data
        except Exception as e:
            self.logger.error(f"Failed to load pickle from GitHub: {e}")
            return None

    def _get_max_date_from_pickle_data(self) -> Optional[datetime]:
        """
        Find the maximum/latest date present in the loaded pickle data.

        Scans through all instruments and their date keys to find the most recent date.

        Returns:
            Optional[datetime]: Latest date found in pickle data, None if no data or error

        Example:
            >>> max_date = manager._get_max_date_from_pickle_data()
            >>> if max_date:
            >>>     print(f"Latest data date: {max_date}")
        """
        if not self.pickle_data:
            return None

        try:
            max_date = None
            for symbol_data in self.pickle_data.values():
                if not isinstance(symbol_data, dict):
                    continue
                
                # Extract all date keys
                date_keys = []
                for key in symbol_data.keys():
                    if isinstance(key, (datetime, pd.Timestamp)):
                        date_keys.append(key)
                    elif isinstance(key, str):
                        try:
                            date_keys.append(datetime.strptime(key.split("T")[0], '%Y-%m-%d'))
                        except ValueError:
                            continue
                
                if date_keys:
                    symbol_max = max(date_keys)
                    if max_date is None or symbol_max > max_date:
                        max_date = symbol_max

            return max_date
        except Exception as e:
            self.logger.error(f"Error finding max date from pickle data: {e}")
            return None

    def _get_recent_data_from_kite(self, start_date: datetime) -> Optional[Dict]:
        """
        Fetch market data from Kite API starting from the specified date.

        Args:
            start_date: Starting date for data fetch (inclusive)

        Returns:
            Optional[Dict]: Recent market data dictionary if successful, None otherwise

        Example:
            >>> start_date = datetime(2023, 12, 20)
            >>> recent_data = manager._get_recent_data_from_kite(start_date)
            >>> if recent_data:
            >>>     print(f"Got {len(recent_data)} recent data points from Kite")
        """
        try:
            from pkbrokers.kite.instrumentHistory import KiteTickerHistory

            kite_history = KiteTickerHistory()

            # Get tradingsymbols from pickle or database
            tradingsymbols = self._get_tradingsymbols()

            if not tradingsymbols:
                self.logger.debug("No tradingsymbols found to fetch data")
                return None

            # Format dates
            start_date_str = self._format_date(start_date)
            end_date_str = self._format_date(datetime.now())
            
            # Fetch historical data
            historical_data = kite_history.get_multiple_instruments_history(
                tradingsymbols=tradingsymbols, 
                from_date=start_date_str, 
                to_date=end_date_str
            )

            # Save to database if available
            if hasattr(kite_history, "_save_to_database") and historical_data:
                kite_history._save_to_database(historical_data, "instrument_history")

            return historical_data

        except ImportError:
            self.logger.debug("KiteTickerHistory module not available")
            return None
        except Exception as e:
            self.logger.debug(f"Error fetching data from Kite: {e}")
            return None

    def _fetch_data_from_database(self, start_date: datetime, end_date: datetime) -> Dict:
        """
        Fetch historical data from instrument_history table for the specified date range.

        Args:
            start_date: Start date for data fetch (inclusive)
            end_date: End date for data fetch (inclusive)

        Returns:
            Dict: Structured historical data with trading symbols as keys

        Example:
            >>> start = datetime(2023, 12, 20)
            >>> end = datetime(2023, 12, 25)
            >>> historical_data = manager._fetch_data_from_database(start, end)
            >>> print(f"Fetched {len(historical_data)} symbols from database")
        """
        if not self._connect_to_database():
            return {}

        try:
            # Format dates
            start_date_str = self._format_date(start_date)
            end_date_str = self._format_date(end_date)
            
            # Fetch instrument history data
            cursor = self.db_conn.cursor()
            query = """
                SELECT ih.*, i.tradingsymbol
                FROM instrument_history ih
                JOIN instruments i ON ih.instrument_token = i.instrument_token
                WHERE ih.timestamp >= ? AND ih.timestamp <= ?
                AND ih.interval = 'day'
            """
            cursor.execute(query, (start_date_str, end_date_str))
            results = cursor.fetchall()

            # Fetch column names
            columns = [desc[0] for desc in cursor.description]

            return self._process_database_data(results, columns)

        except Exception as e:
            self.logger.debug(f"Error fetching data from database: {e}")
            return {}

    def _orchestrate_ticks_download(self) -> bool:
        """
        Trigger the ticks download process using orchestrate_consumer.

        Sends a "/token" command to download ticks.json file to user data directory.

        Returns:
            bool: True if ticks download was successful, False otherwise

        Example:
            >>> success = manager._orchestrate_ticks_download()
            >>> if success:
            >>>     print("Ticks download completed")
        """
        try:
            from pkbrokers.bot.orchestrator import orchestrate_consumer
            
            # Send command to download ticks
            orchestrate_consumer(command="/ticks")
            
            if self.ticks_json_path.exists():
                self.logger.debug("Ticks download completed successfully")
                return True
            else:
                self.logger.debug("Ticks download failed or file not created")
                return False
                
        except ImportError:
            self.logger.debug("orchestrate_consumer not available")
            return False
        except Exception as e:
            self.logger.error(f"Error during ticks download: {e}")
            return False

    def _load_and_process_ticks_json(self) -> Optional[Dict]:
        """
        Load and process data from ticks.json file.

        Reads the ticks.json file, parses its content, and converts it to the same
        format as the pickle data for merging.

        Returns:
            Optional[Dict]: Processed ticks data in pickle-compatible format

        Example:
            >>> ticks_data = manager._load_and_process_ticks_json()
            >>> if ticks_data:
            >>>     print(f"Processed {len(ticks_data)} symbols from ticks.json")
        """
        if not self.ticks_json_path.exists():
            self.logger.debug("ticks.json file not found")
            return None

        try:
            with open(self.ticks_json_path, 'r') as f:
                ticks_data = json.load(f)

            # Convert ticks.json format to pickle data format
            processed_data = {}
            
            for instrument_data in ticks_data.values():
                tradingsymbol = instrument_data.get('trading_symbol')
                if not tradingsymbol:
                    continue
                
                # Extract date from timestamp
                timestamp = instrument_data.get('ohlcv').get("timestamp")
                if not timestamp:
                    continue
                    
                try:
                    # Convert timestamp to date
                    if isinstance(timestamp, str):
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).astimezone(tz=pytz.timezone("Asia/Kolkata"))
                    else:
                        dt = datetime.fromtimestamp(timestamp).astimezone(tz=pytz.timezone("Asia/Kolkata"))

                    date_key = dt.date()
                    
                    # Create or update symbol data
                    if tradingsymbol not in processed_data:
                        processed_data[tradingsymbol] = {}
                    
                    processed_data[tradingsymbol][date_key] = {
                        'open': instrument_data.get('ohlcv').get('open'),
                        'high': instrument_data.get('ohlcv').get('high'),
                        'low': instrument_data.get('ohlcv').get('low'),
                        'close': instrument_data.get('ohlcv').get('close'),
                        'volume': instrument_data.get('ohlcv').get('volume'),
                        # 'oi': instrument_data.get('oi', 0),
                        # 'instrument_token': instrument_data.get('instrument_token'),
                        'timestamp': str(dt),
                        'source': 'ticks.json'  # Mark source for debugging
                    }
                    
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Error processing timestamp {timestamp}: {e}")
                    continue

            return processed_data

        except Exception as e:
            self.logger.error(f"Error loading/processing ticks.json: {e}")
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
                    # "instrument_token": row.get("instrument_token"),
                    "timestamp": PKDateUtilities.utc_str_to_ist(row.get("timestamp"))
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
                    # Update existing symbol data (preserve old, add new)
                    self.pickle_data[tradingsymbol].update(daily_data)
                else:
                    # Add new symbol
                    self.pickle_data[tradingsymbol] = daily_data
        else:
            # Create new pickle data
            self.pickle_data = new_data

        # Ensure directory exists
        self.local_pickle_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to local pickle file
        with open(self.local_pickle_path, "wb") as f:
            pickle.dump(self.pickle_data, f)

        self.logger.debug(f"Pickle file updated successfully: {self.local_pickle_path}")

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

    def execute(self, fetch_kite=False) -> bool:
        """
        Main execution method that orchestrates the complete data synchronization process.

        Workflow:
        1. Check if pickle file exists locally in user data directory
        2. If local file exists: load from local
        3. If local file doesn't exist: check GitHub and download if available
        4. If no pickle available anywhere: fetch full year from database
        5. Find latest date in existing data
        6. Fetch incremental data from latest date until today from multiple sources
        7. Download and process ticks.json data
        8. Update local pickle file with all new data

        Returns:
            bool: True if data was successfully loaded/created, False otherwise

        Example:
            >>> manager = InstrumentDataManager()
            >>> success = manager.execute()
            >>> if success:
            >>>     print("Data synchronization completed successfully")
        """
        self.logger.debug("Starting data synchronization process...")

        # Step 1: Load pickle data (local first, then remote if needed)
        if self._check_pickle_exists_locally():
            self.logger.debug("Pickle file found locally, loading...")
            if not self._load_pickle_from_local():
                self.logger.debug("Failed to load local pickle, checking GitHub...")
                if self._check_pickle_exists_remote():
                    self._load_pickle_from_github()
        elif self._check_pickle_exists_remote():
            self.logger.debug("Pickle file found on GitHub, downloading...")
            self._load_pickle_from_github()
        else:
            self.logger.debug("No pickle file found locally or remotely")

        # Step 2: If no data loaded, fetch full year from database
        if not self.pickle_data:
            self.logger.debug("Fetching full year data from database...")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            historical_data = self._fetch_data_from_database(start_date, end_date)
            
            if historical_data:
                self.pickle_data = historical_data
                self._update_pickle_file({})  # Save initial data
                self.logger.debug("Initial pickle file created from database data")
            else:
                self.logger.debug("No data available from database")
                return False

        # Step 3: Find latest date and fetch incremental data
        max_date = self._get_max_date_from_pickle_data()
        today = datetime.now().date()
        
        if max_date and max_date.date() < today:
            self.logger.debug(f"Fetching incremental data from {max_date.date()} to {today}")
            
            # Convert max_date to datetime for calculations
            if isinstance(max_date, datetime):
                start_datetime = max_date
            else:
                start_datetime = datetime.combine(max_date, datetime.min.time())
            
            # Add one day to start from the next day
            start_datetime += timedelta(days=1)
            
            # Fetch from multiple sources (prioritized)
            incremental_data = {}
            
            if fetch_kite:
                # Try Kite API first
                kite_data = self._get_recent_data_from_kite(start_datetime)
                if kite_data:
                    incremental_data.update(kite_data)
                    self.logger.debug(f"Added {len(kite_data)} symbols from Kite API")
            
            # Try database next
            if not incremental_data:
                db_data = self._fetch_data_from_database(start_datetime, datetime.now())
                if db_data:
                    incremental_data.update(db_data)
                    self.logger.debug(f"Added {len(db_data)} symbols from database")
                        
            # Update pickle with incremental data
            if incremental_data:
                self._update_pickle_file(incremental_data)
                self.logger.debug(f"Updated with {len(incremental_data)} incremental records")
        
        # Step 4: Download and process ticks.json
        self.logger.debug("Initiating ticks download...")
        if self._orchestrate_ticks_download():
            ticks_data = self._load_and_process_ticks_json()
            if ticks_data:
                self._update_pickle_file(ticks_data)
                self.logger.debug(f"Updated with {len(ticks_data)} records from ticks.json")
        
        self.logger.debug("Data synchronization process completed")
        return self.pickle_data is not None
