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

import json
import pickle
import sqlite3
from datetime import date, datetime, timedelta, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import libsql
import numpy as np
import pandas as pd
import pytz
import requests
from PKDevTools.classes import Archiver
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities

from pkbrokers.kite.threadSafeDatabase import DEFAULT_DB_PATH


class InstrumentDataManager:
    """
    A comprehensive data manager for financial instrument data synchronization and retrieval.

    This class handles data from multiple sources including local/remote pickle files,
    remote databases (Turso/SQLite), Kite API, and ticks.json files. It provides seamless
    data synchronization, updating, and retrieval for financial analysis and screening.

    The class now saves data in a symbol-indexed format where each symbol key contains
    a dictionary with 'data', 'columns', and 'index' keys for direct DataFrame creation.

    Key Features:
    - Local-first approach: Checks for pickle file in user data directory first
    - Incremental updates: Fetches only missing data from the latest available date
    - Multi-source integration: Supports Turso DB, SQLite, Kite API, and ticks.json
    - Automated synchronization: Orchestrates complete data update pipeline
    - DataFrame-compatible format: Directly loadable into pandas DataFrame
    - Symbol-based access: Direct access to symbol data via pickle_data["SYMBOL"]

    Attributes:
        pickle_url (str): GitHub repository URL for the pickle file
        raw_pickle_url (str): Raw content URL for the pickle file
        db_conn: Database connection object
        pickle_data (Dict): Loaded pickle data with symbol-indexed DataFrame-compatible format
        logger: Logger instance for debugging and information
        local_pickle_path (Path): Local path to pickle file in user data directory
        ticks_json_path (Path): Local path to ticks.json file

    Example:
        >>> from pkbrokers.kite.datamanager import InstrumentDataManager
        >>> manager = InstrumentDataManager()
        >>> success = manager.execute()
        >>> if success:
        >>>     # Directly create DataFrame from symbol data
        >>>     reliance_data = manager.pickle_data["RELIANCE"]
        >>>     df = pd.DataFrame(
        >>>         data=reliance_data['data'],
        >>>         columns=reliance_data['columns'],
        >>>         index=reliance_data['index']
        >>>     )
        >>>     print(f"Reliance DataFrame shape: {df.shape}")
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
        self.local_pickle_path = (
            Path(Archiver.get_user_data_dir()) / self.pickle_file_name
        )
        self.ticks_json_path = Path(Archiver.get_user_data_dir()) / "ticks.json"
        self.pickle_url = f"https://github.com/pkjmesra/PKScreener/tree/actions-data-download/results/Data/{path}"
        self.raw_pickle_url = f"https://raw.githubusercontent.com/pkjmesra/PKScreener/refs/heads/actions-data-download/results/Data/{path}"
        self.db_conn = None
        self.pickle_data = None
        self.db_type = "turso" or PKEnvironment().DB_TYPE
        self.logger = default_logger()

    def _is_symbol_dataframe_format(self, data: Any) -> bool:
        """
        Check if data is in symbol-indexed DataFrame-compatible format.
        
        Args:
            data: Data to check
            
        Returns:
            bool: True if data is in symbol-indexed DataFrame format, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        # Check if it's symbol-indexed format
        for symbol, symbol_data in data.items():
            if not isinstance(symbol_data, dict):
                return False
            if not all(key in symbol_data for key in ['data', 'columns', 'index']):
                return False
        return True

    def _is_old_format(self, data: Any) -> bool:
        """
        Check if data is in the old format {symbol: {date: {ohlcv_data}}}.
        
        Args:
            data: Data to check
            
        Returns:
            bool: True if data is in old format, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        for symbol, symbol_data in data.items():
            if not isinstance(symbol_data, dict):
                return False
            for date_key, ohlcv_data in symbol_data.items():
                if not isinstance(ohlcv_data, dict):
                    return False
                if not all(key in ohlcv_data for key in ['open', 'high', 'low', 'close', 'volume']):
                    return False
        return True

    def _normalize_timestamp(self, timestamp_obj: Union[date, datetime, str]) -> str:
        """
        Convert various timestamp formats to consistent ISO format string with timezone.
        
        Preserves complete time and timezone information. All timestamps are converted
        to Asia/Kolkata timezone for consistency.
        
        Args:
            timestamp_obj: Timestamp in various formats (date, datetime, str)
            
        Returns:
            str: ISO format timestamp string with timezone (e.g., "2023-12-25T15:30:45+05:30")
        """
        try:
            kolkata_tz = pytz.timezone("Asia/Kolkata")
            
            if isinstance(timestamp_obj, datetime):
                # Handle datetime object
                if timestamp_obj.tzinfo is None:
                    timestamp_obj = timestamp_obj.replace(tzinfo=pytz.UTC)
                return timestamp_obj.astimezone(kolkata_tz).isoformat()
                
            elif isinstance(timestamp_obj, date):
                # Handle date object - create datetime at market open (9:15 AM)
                dt = datetime.combine(timestamp_obj, time(9, 15, 0))
                dt_kolkata = kolkata_tz.localize(dt)
                return dt_kolkata.isoformat()
                
            elif isinstance(timestamp_obj, str):
                # Handle string timestamp
                try:
                    # Try ISO format first
                    if 'T' in timestamp_obj:
                        dt = datetime.fromisoformat(timestamp_obj.replace('Z', '+00:00'))
                    else:
                        # Try various string formats
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d']:
                            try:
                                dt = datetime.strptime(timestamp_obj, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            raise ValueError(f"Unknown timestamp format: {timestamp_obj}")
                    
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=pytz.UTC)
                    return dt.astimezone(kolkata_tz).isoformat()
                    
                except ValueError as e:
                    self.logger.warning(f"Could not parse timestamp string '{timestamp_obj}': {e}")
                    return timestamp_obj
                    
            else:
                self.logger.warning(f"Unsupported timestamp type: {type(timestamp_obj)}")
                return str(timestamp_obj)
                
        except Exception as e:
            self.logger.error(f"Error normalizing timestamp {timestamp_obj}: {e}")
            return str(timestamp_obj)

    def _convert_old_format_to_symbol_dataframe_format(self, old_format_data: Dict) -> Dict[str, Any]:
        """
        Convert old format data to symbol-indexed DataFrame-compatible format.
        
        Args:
            old_format_data: Dictionary in old format {symbol: {date: {ohlcv_data}}}
            
        Returns:
            Dict: Symbol-indexed dictionary with each symbol containing 'data', 'columns', and 'index'
        """
        if not old_format_data:
            return {}

        symbol_dataframe_format = {}
        
        for symbol, symbol_data in old_format_data.items():
            # Collect all timestamps for this symbol
            timestamps = []
            data_rows = []
            
            for timestamp_str, ohlcv in symbol_data.items():
                normalized_ts = self._normalize_timestamp(timestamp_str)
                timestamps.append(normalized_ts)
                data_rows.append([
                    ohlcv.get('open'),
                    ohlcv.get('high'),
                    ohlcv.get('low'),
                    ohlcv.get('close'),
                    ohlcv.get('volume')
                ])
            
            # Sort by timestamp
            sorted_indices = sorted(range(len(timestamps)), key=lambda i: timestamps[i])
            sorted_timestamps = [timestamps[i] for i in sorted_indices]
            sorted_data = [data_rows[i] for i in sorted_indices]
            
            symbol_dataframe_format[symbol] = {
                'data': sorted_data,
                'columns': ['open', 'high', 'low', 'close', 'volume'],
                'index': sorted_timestamps
            }
        
        return symbol_dataframe_format

    def _convert_symbol_dataframe_format_to_old_format(self, symbol_dataframe_format: Dict) -> Dict:
        """
        Convert symbol-indexed DataFrame format back to old internal format.
        
        Args:
            symbol_dataframe_format: Dictionary in symbol-indexed DataFrame format
            
        Returns:
            Dict: Old format dictionary {symbol: {date: {ohlcv_data}}}
        """
        if not symbol_dataframe_format:
            return {}

        old_format_data = {}
        
        for symbol, symbol_data in symbol_dataframe_format.items():
            old_format_data[symbol] = {}
            
            data = symbol_data['data']
            index = symbol_data['index']
            columns = symbol_data['columns']
            
            for i, timestamp in enumerate(index):
                ohlcv_data = {}
                for j, col in enumerate(columns):
                    ohlcv_data[col] = data[i][j] if i < len(data) and j < len(data[i]) else None
                
                old_format_data[symbol][timestamp] = ohlcv_data
        
        return old_format_data

    def _connect_to_database(self) -> bool:
        """
        Establish connection to remote Turso database using libsql.

        Returns:
            bool: True if connection successful, False otherwise
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
        db_path = DEFAULT_DB_PATH
        try:
            if libsql:
                conn = libsql.connect(db_path)
            else:
                conn = sqlite3.connect(db_path, check_same_thread=False)

            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size = -100000")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 30000000000")
            return conn
        except Exception as e:
            self.logger.error(f"Failed to create local connection: {str(e)}")
            raise

    def _create_turso_connection(self):
        """Create connection to Turso database using libSQL"""
        try:
            if not libsql:
                raise ImportError("libsql_experimental package is required for Turso support")

            url = PKEnvironment().TDU
            auth_token = PKEnvironment().TAT

            if not url or not auth_token:
                raise ValueError("Turso configuration requires both URL and auth token")

            conn = libsql.connect(database=url, auth_token=auth_token)
            return conn

        except Exception as e:
            self.logger.error(f"Failed to create Turso connection: {str(e)}")
            raise

    def _check_pickle_exists_locally(self) -> bool:
        """
        Check if the pickle file exists in the local user data directory.

        Returns:
            bool: True if file exists locally, False otherwise
        """
        return (
            self.local_pickle_path.exists()
            and self.local_pickle_path.stat().st_size > 0
        )

    def _check_pickle_exists_remote(self) -> bool:
        """
        Check if the pickle file exists on GitHub repository.

        Returns:
            bool: True if file exists (HTTP 200), False otherwise
        """
        try:
            response = requests.head(self.raw_pickle_url)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def _load_pickle_from_local(self) -> Optional[Dict]:
        """
        Load pickle data from local file with format detection and conversion.
        """
        try:
            with open(self.local_pickle_path, "rb") as f:
                loaded_data = pickle.load(f)

            # Handle different formats
            if self._is_symbol_dataframe_format(loaded_data):
                self.pickle_data = loaded_data
                self.logger.info("Loaded data in symbol-indexed DataFrame format from local file")
                
            elif self._is_old_format(loaded_data):
                self.logger.info("Converting old format to symbol-indexed DataFrame format")
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(loaded_data)
                self._save_pickle_file()
                
            else:
                self.logger.error("Unknown data format in local pickle file")
                return None

            return self.pickle_data
            
        except Exception as e:
            self.logger.error(f"Failed to load local pickle file: {e}")
            return None

    def _load_pickle_from_github(self) -> Optional[Dict]:
        """
        Download and load pickle data from GitHub.
        """
        try:
            response = requests.get(self.raw_pickle_url)
            response.raise_for_status()
            
            self.local_pickle_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.local_pickle_path, "wb") as f:
                f.write(response.content)
            
            loaded_data = pickle.loads(response.content)
            
            if self._is_symbol_dataframe_format(loaded_data):
                self.pickle_data = loaded_data
                self.logger.info("Loaded data in symbol-indexed DataFrame format from GitHub")
                
            elif self._is_old_format(loaded_data):
                self.logger.info("Converting old format to symbol-indexed DataFrame format from GitHub")
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(loaded_data)
                self._save_pickle_file()
                
            else:
                self.logger.error("Unknown data format in GitHub pickle file")
                return None

            return self.pickle_data
            
        except Exception as e:
            self.logger.error(f"Failed to load pickle from GitHub: {e}")
            return None

    def _save_pickle_file(self):
        """Save data to pickle file in symbol-indexed DataFrame format."""
        if self.pickle_data is None:
            self.logger.warning("No data to save")
            return

        self.local_pickle_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.local_pickle_path, "wb") as f:
            pickle.dump(self.pickle_data, f)
            
        self.logger.info(f"Pickle file saved: {self.local_pickle_path}")

    def _get_max_date_from_pickle_data(self) -> Optional[datetime]:
        """
        Find the maximum/latest timestamp in the loaded data.
        """
        if not self.pickle_data:
            return None

        try:
            max_datetime = None
            
            for symbol_data in self.pickle_data.values():
                for timestamp_str in symbol_data['index']:
                    try:
                        dt = datetime.fromisoformat(timestamp_str)
                        if max_datetime is None or dt > max_datetime:
                            max_datetime = dt
                    except ValueError:
                        continue
            
            return max_datetime
            
        except Exception as e:
            self.logger.error(f"Error finding max date: {e}")
            return None

    def _get_recent_data_from_kite(self, start_date: datetime) -> Optional[Dict]:
        """
        Fetch market data from Kite API starting from the specified date.

        Args:
            start_date: Starting date for data fetch (inclusive)

        Returns:
            Optional[Dict]: Recent market data dictionary if successful, None otherwise
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
                to_date=end_date_str,
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

    def _fetch_data_from_database(
        self, start_date: datetime, end_date: datetime
    ) -> Dict:
        """
        Fetch historical data from instrument_history table for the specified date range.

        Args:
            start_date: Start date for data fetch (inclusive)
            end_date: End date for data fetch (inclusive)

        Returns:
            Dict: Structured historical data with trading symbols as keys
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

        Returns:
            bool: True if ticks download was successful, False otherwise
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
        Preserves full timestamp with timezone information.

        Returns:
            Optional[Dict]: Processed ticks data in old format
        """
        if not self.ticks_json_path.exists():
            self.logger.error("ticks.json file not found")
            return None

        try:
            with open(self.ticks_json_path, "r") as f:
                ticks_data = json.load(f)

            # Convert ticks.json format to old format
            processed_data = {}

            for instrument_data in ticks_data.values():
                tradingsymbol = instrument_data.get("trading_symbol")
                if not tradingsymbol:
                    continue

                # Extract timestamp
                timestamp = instrument_data.get("ohlcv").get("timestamp")
                if not timestamp:
                    continue

                try:
                    # Convert timestamp to datetime with timezone
                    if isinstance(timestamp, str):
                        dt = datetime.fromisoformat(
                            timestamp.replace("Z", "+00:00")
                        ).astimezone(tz=pytz.timezone("Asia/Kolkata"))
                    else:
                        dt = datetime.fromtimestamp(timestamp).astimezone(
                            tz=pytz.timezone("Asia/Kolkata")
                        )

                    # Use full ISO format timestamp as key
                    timestamp_key = dt.isoformat()

                    # Create or update symbol data
                    if tradingsymbol not in processed_data:
                        processed_data[tradingsymbol] = {}

                    processed_data[tradingsymbol][timestamp_key] = {
                        "open": instrument_data.get("ohlcv").get("open"),
                        "high": instrument_data.get("ohlcv").get("high"),
                        "low": instrument_data.get("ohlcv").get("low"),
                        "close": instrument_data.get("ohlcv").get("close"),
                        "volume": instrument_data.get("ohlcv").get("volume")
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
        """
        if isinstance(date, datetime):
            return date.strftime("%Y-%m-%d")
        return date

    def _get_tradingsymbols(self) -> List[str]:
        """
        Retrieve list of trading symbols from available data sources.

        Returns:
            List[str]: List of trading symbols
        """
        if self.pickle_data:
            return list(self.pickle_data.keys())
        else:
            return self._get_tradingsymbols_from_db()

    def _get_tradingsymbols_from_db(self) -> List[str]:
        """
        Fetch distinct trading symbols from instruments database table.

        Returns:
            List[str]: List of unique trading symbols from database
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
        Preserves full timestamp information.
        
        Args:
            results: Raw database query results
            columns: Column names from database query

        Returns:
            Dict: Processed data in old format
        """
        master_data = {}

        # Convert to DataFrame for easier processing
        df = pd.DataFrame(results, columns=columns)

        if df.empty:
            return master_data

        # Group by tradingsymbol and process
        for tradingsymbol, group in df.groupby("tradingsymbol"):
            # Convert to old format with full timestamp as key
            symbol_data = {}
            for _, row in group.iterrows():
                timestamp = row.get("timestamp")
                
                # Convert timestamp to ISO format string
                if hasattr(timestamp, "isoformat"):
                    timestamp_key = timestamp.isoformat()
                else:
                    # Try to parse string timestamp
                    try:
                        if isinstance(timestamp, str):
                            if 'T' in timestamp:
                                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            else:
                                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                            timestamp_key = dt.isoformat()
                        else:
                            timestamp_key = str(timestamp)
                    except ValueError:
                        self.logger.error(f"Could not parse timestamp: {timestamp}")
                        continue

                symbol_data[timestamp_key] = {
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "volume": row.get("volume")
                }

            master_data[tradingsymbol] = symbol_data

        return master_data

    def _update_pickle_file(self, new_data: Dict):
        """
        Update local pickle file with new data, merging with existing data.

        Args:
            new_data: Dictionary containing new data to merge (in old format)
        """
        # Convert new data to symbol-indexed DataFrame format
        new_data_symbol_format = self._convert_old_format_to_symbol_dataframe_format(new_data)
        
        if self.pickle_data:
            # Merge new data with existing data
            for symbol, symbol_data in new_data_symbol_format.items():
                if symbol in self.pickle_data:
                    # Merge timestamps and data
                    existing_data = self.pickle_data[symbol]
                    
                    # Combine indices and data
                    combined_indices = list(existing_data['index']) + list(symbol_data['index'])
                    combined_data = existing_data['data'] + symbol_data['data']
                    
                    # Sort by timestamp
                    sorted_indices = sorted(range(len(combined_indices)), key=lambda i: combined_indices[i])
                    sorted_indices_list = [combined_indices[i] for i in sorted_indices]
                    sorted_data = [combined_data[i] for i in sorted_indices]
                    
                    # Remove duplicates
                    unique_indices = []
                    unique_data = []
                    seen_indices = set()
                    
                    for i, timestamp in enumerate(sorted_indices_list):
                        if timestamp not in seen_indices:
                            seen_indices.add(timestamp)
                            unique_indices.append(timestamp)
                            unique_data.append(sorted_data[i])
                    
                    self.pickle_data[symbol] = {
                        'data': unique_data,
                        'columns': ['open', 'high', 'low', 'close', 'volume'],
                        'index': unique_indices
                    }
                else:
                    # Add new symbol
                    self.pickle_data[symbol] = symbol_data
        else:
            # Create new pickle data
            self.pickle_data = new_data_symbol_format

        # Save the updated data
        self._save_pickle_file()
        self.logger.info(f"Pickle file updated successfully: {self.local_pickle_path}")

    def get_data_for_symbol(self, tradingsymbol: str) -> Optional[Dict]:
        """
        Retrieve data for a specific trading symbol in DataFrame-compatible format.

        Args:
            tradingsymbol: Trading symbol to retrieve data for (e.g., "RELIANCE")

        Returns:
            Optional[Dict]: Data for the specified symbol if available, None otherwise
        """
        if not self.pickle_data:
            return None

        return self.pickle_data.get(tradingsymbol)

    def get_dataframe_for_symbol(self, tradingsymbol: str) -> Optional[pd.DataFrame]:
        """
        Return the data for a specific symbol as a pandas DataFrame.

        Args:
            tradingsymbol: Trading symbol to retrieve data for

        Returns:
            Optional[pd.DataFrame]: DataFrame containing the symbol data, or None if not available
        """
        symbol_data = self.get_data_for_symbol(tradingsymbol)
        if not symbol_data:
            return None

        return pd.DataFrame(
            data=symbol_data['data'],
            columns=symbol_data['columns'],
            index=symbol_data['index']
        )

    def convert_old_pickle_to_symbol_dataframe_format(
        self, file_path: Union[str, Path]
    ) -> bool:
        """
        Convert an old format pickle file to the new symbol-indexed DataFrame-compatible format.

        Args:
            file_path: Path to the old format pickle file

        Returns:
            bool: True if conversion was successful, False otherwise
        """
        try:
            # Load the old format data
            with open(file_path, "rb") as f:
                old_data = pickle.load(f)

            # Convert to new format
            new_format_data = self._convert_old_format_to_symbol_dataframe_format(old_data)

            # Save in new format
            new_file_path = Path(file_path).with_name(
                f"symbol_format_{Path(file_path).name}"
            )
            with open(new_file_path, "wb") as f:
                pickle.dump(new_format_data, f)

            self.logger.info(f"Converted {file_path} to symbol-indexed format: {new_file_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to convert pickle file: {e}")
            return False

    def execute(self, fetch_kite=False) -> bool:
        """
        Main execution method that orchestrates the complete data synchronization process.

        Returns:
            bool: True if data was successfully loaded/created, False otherwise
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
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(historical_data)
                self._save_pickle_file()
                self.logger.debug("Initial pickle file created from database data")
            else:
                self.logger.debug("No data available from database")
                return False

        # Step 3: Find latest date and fetch incremental data
        max_date = self._get_max_date_from_pickle_data()
        today = datetime.now().date()

        if max_date and max_date.date() < today:
            self.logger.debug(
                f"Fetching incremental data from {max_date.date()} to {today}"
            )

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
                self.logger.debug(
                    f"Updated with {len(incremental_data)} incremental records"
                )

        # Step 4: Download and process ticks.json
        self.logger.debug("Initiating ticks download...")
        if self._orchestrate_ticks_download():
            ticks_data = self._load_and_process_ticks_json()
            if ticks_data:
                self._update_pickle_file(ticks_data)
                self.logger.debug(
                    f"Updated with {len(ticks_data)} records from ticks.json"
                )

        self.logger.debug("Data synchronization process completed")
        return self.pickle_data is not None