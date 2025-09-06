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
from typing import Dict, List, Optional, Union, Any
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

    The class now saves data in a DataFrame-compatible format with separate keys for:
    - 'data': 2D array of values
    - 'columns': Column names
    - 'index': Index values (typically dates)

    Key Features:
    - Local-first approach: Checks for pickle file in user data directory first
    - Incremental updates: Fetches only missing data from the latest available date
    - Multi-source integration: Supports Turso DB, SQLite, Kite API, and ticks.json
    - Automated synchronization: Orchestrates complete data update pipeline
    - DataFrame-compatible format: Directly loadable into pandas DataFrame
    - Backward compatibility: Can read and convert old format pickle files

    Attributes:
        pickle_url (str): GitHub repository URL for the pickle file
        raw_pickle_url (str): Raw content URL for the pickle file
        db_conn: Database connection object
        pickle_data (Dict): Loaded pickle data in DataFrame-compatible format
        logger: Logger instance for debugging and information
        local_pickle_path (Path): Local path to pickle file in user data directory
        ticks_json_path (Path): Local path to ticks.json file

    Example:
        >>> from pkbrokers.kite.datamanager import InstrumentDataManager
        >>> manager = InstrumentDataManager()
        >>> success = manager.execute()
        >>> if success:
        >>>     # Directly create DataFrame from pickle data
        >>>     df = pd.DataFrame(
        >>>         data=manager.pickle_data['data'],
        >>>         columns=manager.pickle_data['columns'],
        >>>         index=manager.pickle_data['index']
        >>>     )
        >>>     print(f"DataFrame shape: {df.shape}")
    Another Example:
        >>> # Initialize the manager
        >>> manager = InstrumentDataManager()
        >>> # Convert an old format pickle file to the new format
        >>> manager.convert_old_pickle_to_dataframe_format("old_data.pkl")
        >>> # Execute the data synchronization process
        >>> success = manager.execute()
        >>> if success:
        >>>     # Get data as a DataFrame
        >>>     df = manager.get_dataframe()
        >>>     print(f"DataFrame shape: {df.shape}")
        >>>     # Access data for a specific symbol
        >>>     reliance_data = manager.get_data_for_symbol("RELIANCE")
        >>>     print(f"Reliance has {len(reliance_data)} days of data")
        >>>     # Direct access to the DataFrame-compatible format
        >>>     print(f"Data array shape: {len(manager.pickle_data['data'])} rows")
        >>>     print(f"Number of columns: {len(manager.pickle_data['columns'])}")
        >>>     print(f"Number of index values: {len(manager.pickle_data['index'])}")
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

    def _is_dataframe_format(self, data: Any) -> bool:
        """
        Check if the data is in the new DataFrame-compatible format.
        
        Args:
            data: Data to check
            
        Returns:
            bool: True if data is in DataFrame format, False otherwise
            
        Example:
            >>> is_new_format = self._is_dataframe_format(loaded_data)
        """
        return (isinstance(data, dict) and 
                'data' in data and 
                'columns' in data and 
                'index' in data)

    def _convert_old_format_to_dataframe_format(self, old_format_data: Dict) -> Dict[str, Any]:
        """
        Convert the old format data to DataFrame-compatible format.
        
        Args:
            old_format_data: Dictionary in the old format {symbol: {date: {ohlcv_data}}}
            
        Returns:
            Dict: DataFrame-compatible dictionary with 'data', 'columns', and 'index' keys
            
        Example:
            >>> new_format_data = self._convert_old_format_to_dataframe_format(old_data)
        """
        if not old_format_data:
            return {'data': [], 'columns': [], 'index': []}
        
        # Collect all unique dates and symbols
        all_dates = set()
        all_symbols = set(old_format_data.keys())
        
        for symbol_data in old_format_data.values():
            all_dates.update(symbol_data.keys())
        
        # Sort dates and symbols
        sorted_dates = sorted(all_dates)
        sorted_symbols = sorted(all_symbols)
        
        # Create multi-index columns (symbol, ohlcv field)
        columns = pd.MultiIndex.from_product(
            [sorted_symbols, ['open', 'high', 'low', 'close', 'volume']],
            names=['symbol', 'field']
        )
        
        # Create 2D data array
        data = []
        for date in sorted_dates:
            row = []
            for symbol in sorted_symbols:
                if date in old_format_data.get(symbol, {}):
                    ohlcv = old_format_data[symbol][date]
                    row.extend([
                        ohlcv.get('open', None),
                        ohlcv.get('high', None),
                        ohlcv.get('low', None),
                        ohlcv.get('close', None),
                        ohlcv.get('volume', None)
                    ])
                else:
                    # Add None values for missing data
                    row.extend([None, None, None, None, None])
            data.append(row)
        
        return {
            'data': data,
            'columns': columns,
            'index': sorted_dates
        }

    def _convert_dataframe_format_to_old_format(self, df_format: Dict[str, Any]) -> Dict:
        """
        Convert DataFrame-compatible format back to the old internal dictionary format.
        
        Args:
            df_format: Dictionary with 'data', 'columns', and 'index' keys
            
        Returns:
            Dict: Old format dictionary {symbol: {date: {ohlcv_data}}}
            
        Example:
            >>> old_format_data = self._convert_dataframe_format_to_old_format(new_data)
        """
        if not df_format or not df_format.get('data'):
            return {}
        
        data = df_format['data']
        columns = df_format['columns']
        index = df_format['index']
        
        # Reconstruct old format
        old_format_data = {}
        
        # Process each column to extract symbol and field
        for col_idx, col in enumerate(columns):
            symbol, field = col
            if symbol not in old_format_data:
                old_format_data[symbol] = {}
            
            # Add data for each date
            for row_idx, date in enumerate(index):
                if date not in old_format_data[symbol]:
                    old_format_data[symbol][date] = {}
                
                old_format_data[symbol][date][field] = data[row_idx][col_idx]
        
        return old_format_data

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
            >>>     print(f"Loaded pickle data with {len(data.get('data', []))} rows from local file")
        """
        try:
            with open(self.local_pickle_path, 'rb') as f:
                loaded_data = pickle.load(f)
            
            # Handle both old and new formats
            if self._is_dataframe_format(loaded_data):
                # Data is already in the new format
                self.pickle_data = loaded_data
                self.logger.debug("Loaded data in DataFrame format from local file")
            else:
                # Data is in old format, convert to new format
                self.logger.info("Converting old format data to DataFrame format")
                self.pickle_data = self._convert_old_format_to_dataframe_format(loaded_data)
                # Save in new format for future use
                self._save_pickle_file()
                
            self.logger.info(f"Loaded pickle data from local file: {self.local_pickle_path}")
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
            >>>     print(f"Loaded pickle data with {len(data.get('data', []))} rows from GitHub")
        """
        try:
            response = requests.get(self.raw_pickle_url)
            response.raise_for_status()
            
            # Ensure directory exists
            self.local_pickle_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to local file first
            with open(self.local_pickle_path, 'wb') as f:
                f.write(response.content)
            
            # Load the data
            loaded_data = pickle.loads(response.content)
            
            # Handle both old and new formats
            if self._is_dataframe_format(loaded_data):
                # Data is already in the new format
                self.pickle_data = loaded_data
                self.logger.info("Loaded data in DataFrame format from GitHub")
            else:
                # Data is in old format, convert to new format
                self.logger.info("Converting old format data to DataFrame format from GitHub")
                self.pickle_data = self._convert_old_format_to_dataframe_format(loaded_data)
                # Save in new format for future use
                self._save_pickle_file()
                
            self.logger.info(f"Downloaded and loaded pickle data from GitHub: {self.raw_pickle_url}")
            return self.pickle_data
        except Exception as e:
            self.logger.error(f"Failed to load pickle from GitHub: {e}")
            return None

    def _save_pickle_file(self):
        """
        Save the current pickle data to file in DataFrame-compatible format.
        
        Example:
            >>> self._save_pickle_file()
        """
        if self.pickle_data is None:
            self.logger.warning("No data to save")
            return
            
        # Ensure directory exists
        self.local_pickle_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to local pickle file
        with open(self.local_pickle_path, "wb") as f:
            pickle.dump(self.pickle_data, f)
            
        self.logger.info(f"Pickle file saved successfully: {self.local_pickle_path}")

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
            # Convert to old format to extract dates (easier to work with)
            old_format_data = self._convert_dataframe_format_to_old_format(self.pickle_data)
            
            max_date = None
            for symbol_data in old_format_data.values():
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
                self.logger.info("No tradingsymbols found to fetch data")
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
            self.logger.error("KiteTickerHistory module not available")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching data from Kite: {e}")
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
            self.logger.error(f"Error fetching data from database: {e}")
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
                self.logger.error("Ticks download failed or file not created")
                return False
                
        except ImportError:
            self.logger.error("orchestrate_consumer not available")
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
            self.logger.error("ticks.json file not found")
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
            # Convert to old format to extract symbols
            old_format_data = self._convert_dataframe_format_to_old_format(self.pickle_data)
            return list(old_format_data.keys())
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
            self.logger.error(f"Error fetching tradingsymbols from database: {e}")
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
                    "timestamp": PKDateUtilities.utc_str_to_ist(row.get("timestamp")).strftime('%Y-%m-%d %H:%M:%S')
                }

            master_data[tradingsymbol] = symbol_data

        return master_data

    def _update_pickle_file(self, new_data: Dict):
        """
        Update local pickle file with new data, merging with existing data.

        Args:
            new_data: Dictionary containing new data to merge (in old format)

        Example:
            >>> manager._update_pickle_file(new_data)
            >>> print("Pickle file updated successfully")
        """
        if self.pickle_data:
            # Convert current data to old format for merging
            current_old_format = self._convert_dataframe_format_to_old_format(self.pickle_data)
            
            # Merge new data with existing data
            for tradingsymbol, daily_data in new_data.items():
                if tradingsymbol in current_old_format:
                    # Update existing symbol data (preserve old, add new)
                    current_old_format[tradingsymbol].update(daily_data)
                else:
                    # Add new symbol
                    current_old_format[tradingsymbol] = daily_data
                    
            # Convert back to DataFrame format
            self.pickle_data = self._convert_old_format_to_dataframe_format(current_old_format)
        else:
            # Create new pickle data
            self.pickle_data = self._convert_old_format_to_dataframe_format(new_data)

        # Save the updated data
        self._save_pickle_file()
        self.logger.info(f"Pickle file updated successfully: {self.local_pickle_path}")

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
        if not self.pickle_data:
            return None
            
        # Convert to old format to extract symbol data
        old_format_data = self._convert_dataframe_format_to_old_format(self.pickle_data)
        return old_format_data.get(tradingsymbol)

    def get_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Return the loaded data as a pandas DataFrame.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing all instrument data, or None if no data loaded

        Example:
            >>> df = manager.get_dataframe()
            >>> if df is not None:
            >>>     print(f"DataFrame shape: {df.shape}")
        """
        if not self.pickle_data:
            return None
            
        return pd.DataFrame(
            data=self.pickle_data['data'],
            columns=self.pickle_data['columns'],
            index=self.pickle_data['index']
        )

    def convert_old_pickle_to_dataframe_format(self, file_path: Union[str, Path]) -> bool:
        """
        Convert an old format pickle file to the new DataFrame-compatible format.
        
        Args:
            file_path: Path to the old format pickle file
            
        Returns:
            bool: True if conversion was successful, False otherwise
            
        Example:
            >>> success = manager.convert_old_pickle_to_dataframe_format("old_data.pkl")
        """
        try:
            # Load the old format data
            with open(file_path, 'rb') as f:
                old_data = pickle.load(f)
                
            # Convert to new format
            new_format_data = self._convert_old_format_to_dataframe_format(old_data)
            
            # Save in new format
            new_file_path = Path(file_path).with_name(f"new_format_{Path(file_path).name}")
            with open(new_file_path, 'wb') as f:
                pickle.dump(new_format_data, f)
                
            self.logger.info(f"Converted {file_path} to new format: {new_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to convert pickle file: {e}")
            return False

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
            self.logger.info("Pickle file found locally, loading...")
            if not self._load_pickle_from_local():
                self.logger.info("Failed to load local pickle, checking GitHub...")
                if self._check_pickle_exists_remote():
                    self._load_pickle_from_github()
        elif self._check_pickle_exists_remote():
            self.logger.info("Pickle file found on GitHub, downloading...")
            self._load_pickle_from_github()
        else:
            self.logger.info("No pickle file found locally or remotely")

        # Step 2: If no data loaded, fetch full year from database
        if not self.pickle_data:
            self.logger.debug("Fetching full year data from database...")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            historical_data = self._fetch_data_from_database(start_date, end_date)
            
            if historical_data:
                self.pickle_data = self._convert_old_format_to_dataframe_format(historical_data)
                self._save_pickle_file()  # Save initial data
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

