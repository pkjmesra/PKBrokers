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
import numpy as np
import libsql
import pandas as pd
import pytz
import requests
from PKDevTools.classes import Archiver
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger

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
        exists, path = Archiver.afterMarketStockDataExists(date_suffix=True)
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
        self._pickle_data_loaded = False
        self._pickle_data = None
        self._db_blocked = False  # Flag to track if database access is blocked (quota exceeded)
        self._local_candle_db = None  # Local SQLite database instance

    def _get_local_candle_db(self):
        """Get or create the local candle database instance."""
        if self._local_candle_db is None:
            try:
                from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
                self._local_candle_db = LocalCandleDatabase()
            except ImportError:
                self.logger.debug("LocalCandleDatabase not available")
            except Exception as e:
                self.logger.debug(f"Failed to initialize LocalCandleDatabase: {e}")
        return self._local_candle_db

    def _fetch_data_from_local_sqlite(self, start_date=None, end_date=None) -> Optional[Dict]:
        """
        Fetch data from local SQLite database as fallback.
        
        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch
            
        Returns:
            Dict: Symbol-indexed data compatible with pickle format
        """
        try:
            local_db = self._get_local_candle_db()
            if local_db is None:
                return None
            
            # Format dates
            start_str = start_date.strftime('%Y-%m-%d') if start_date else None
            end_str = end_date.strftime('%Y-%m-%d') if end_date else None
            
            # Get daily candles from local database
            daily_data = local_db.get_daily_candles(
                start_date=start_str,
                end_date=end_str
            )
            
            if not daily_data:
                self.logger.debug("No data in local SQLite database")
                return None
            
            # Convert to pickle-compatible format
            result = {}
            for symbol, df in daily_data.items():
                if df is not None and not df.empty:
                    # Ensure columns match expected format
                    df = df.rename(columns={
                        'open': 'Open', 'high': 'High', 'low': 'Low',
                        'close': 'Close', 'volume': 'Volume'
                    })
                    
                    result[symbol] = {
                        'data': df[['Open', 'High', 'Low', 'Close', 'Volume']].values.tolist(),
                        'columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
                        'index': [d.isoformat() for d in df.index]
                    }
            
            if result:
                self.logger.info(f"Loaded {len(result)} symbols from local SQLite database")
            
            return result if result else None
            
        except Exception as e:
            self.logger.debug(f"Error fetching from local SQLite: {e}")
            return None

    def _update_local_sqlite_from_ticks(self) -> bool:
        """
        Update local SQLite database from InMemoryCandleStore tick data.
        
        Returns:
            bool: True if update was successful
        """
        try:
            local_db = self._get_local_candle_db()
            if local_db is None:
                return False
            
            from pkbrokers.kite.inMemoryCandleStore import get_candle_store
            candle_store = get_candle_store()
            
            # Update local database from ticks
            success = local_db.update_from_ticks(candle_store)
            
            if success:
                self.logger.debug("Updated local SQLite database from tick data")
            
            return success
            
        except Exception as e:
            self.logger.debug(f"Error updating local SQLite from ticks: {e}")
            return False

    def _get_realtime_candle_data(self, interval: str = 'day') -> Optional[Dict]:
        """
        Get real-time candle data from InMemoryCandleStore for a specific interval.
        
        Args:
            interval: Candle interval ('1m', '2m', '3m', '4m', '5m', '10m', '15m', '30m', '60m', 'day')
            
        Returns:
            Dict: Symbol-indexed data compatible with pickle format
        """
        try:
            from pkbrokers.kite.inMemoryCandleStore import get_candle_store
            candle_store = get_candle_store()
            
            symbols = candle_store.get_all_symbols()
            if not symbols:
                self.logger.debug("No symbols in InMemoryCandleStore")
                return None
            
            result = {}
            timezone = pytz.timezone("Asia/Kolkata")
            today = datetime.now(timezone).strftime('%Y-%m-%d')
            
            for symbol in symbols:
                try:
                    # Get current candle for the interval
                    current_candle = candle_store.get_current_candle(
                        trading_symbol=symbol, interval=interval
                    )
                    
                    if current_candle and current_candle.get('tick_count', 0) > 0:
                        # Get timestamp
                        ts = current_candle.get('timestamp', 0)
                        if isinstance(ts, (int, float)):
                            dt = datetime.fromtimestamp(ts, tz=timezone)
                            index_str = dt.isoformat()
                        else:
                            index_str = f"{today}T09:15:00+05:30"
                        
                        result[symbol] = {
                            'data': [[
                                current_candle.get('open', 0),
                                current_candle.get('high', 0),
                                current_candle.get('low', 0) if current_candle.get('low', float('inf')) != float('inf') else current_candle.get('open', 0),
                                current_candle.get('close', 0),
                                current_candle.get('volume', 0)
                            ]],
                            'columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
                            'index': [index_str]
                        }
                except Exception as e:
                    self.logger.debug(f"Error getting candle for {symbol}: {e}")
                    continue
            
            if result:
                self.logger.info(f"Got real-time {interval} candles for {len(result)} symbols")
            
            return result if result else None
            
        except Exception as e:
            self.logger.debug(f"Error getting real-time candle data: {e}")
            return None

    def _merge_realtime_data_with_historical(self, historical_data: Dict, realtime_data: Dict) -> Dict:
        """
        Merge real-time candle data with historical data.
        
        For each symbol:
        - Keep all historical data
        - Update/append today's candle from real-time data
        
        Args:
            historical_data: Symbol-indexed historical data (pickle format)
            realtime_data: Symbol-indexed real-time data from InMemoryCandleStore
            
        Returns:
            Dict: Merged data in pickle format
        """
        if not historical_data:
            return realtime_data or {}
        
        if not realtime_data:
            return historical_data
        
        merged = {}
        timezone = pytz.timezone("Asia/Kolkata")
        today_str = datetime.now(timezone).strftime('%Y-%m-%d')
        
        # Process all symbols from historical data
        for symbol, hist_data in historical_data.items():
            if symbol not in realtime_data:
                merged[symbol] = hist_data
                continue
            
            rt_data = realtime_data[symbol]
            
            try:
                # Get historical data arrays
                hist_rows = hist_data.get('data', [])
                hist_index = hist_data.get('index', [])
                columns = hist_data.get('columns', ['Open', 'High', 'Low', 'Close', 'Volume'])
                
                # Get real-time data
                rt_rows = rt_data.get('data', [])
                rt_index = rt_data.get('index', [])
                
                if not rt_rows:
                    merged[symbol] = hist_data
                    continue
                
                # Check if today's data already exists in historical
                # Remove today's entry if it exists (we'll replace with real-time)
                new_rows = []
                new_index = []
                for idx, row in zip(hist_index, hist_rows):
                    idx_date = idx[:10] if len(idx) >= 10 else idx
                    if idx_date != today_str:
                        new_rows.append(row)
                        new_index.append(idx)
                
                # Append real-time data
                new_rows.extend(rt_rows)
                new_index.extend(rt_index)
                
                merged[symbol] = {
                    'data': new_rows,
                    'columns': columns,
                    'index': new_index
                }
                
            except Exception as e:
                self.logger.debug(f"Error merging data for {symbol}: {e}")
                merged[symbol] = hist_data
        
        # Add symbols that only exist in real-time data
        for symbol, rt_data in realtime_data.items():
            if symbol not in merged:
                merged[symbol] = rt_data
        
        self.logger.info(f"Merged data: {len(merged)} symbols with real-time updates")
        return merged

    def _load_market_hours_data(self) -> bool:
        """
        Load data optimized for market hours.
        
        Priority:
        1. Check if SQLite has sufficient data (100+ symbols)
        2. If yes, load from SQLite and merge with real-time candles
        3. If no, fall back to pickle file
        
        Returns:
            bool: True if data was loaded successfully with sufficient symbols
        """
        self.logger.info("Loading data for market hours (SQLite + real-time candles)...")
        
        MIN_SYMBOLS_THRESHOLD = 100  # Minimum symbols required to use SQLite
        
        # Step 1: Load historical data from SQLite
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        historical_data = self._fetch_data_from_local_sqlite(start_date, end_date)
        
        # Check if SQLite has sufficient data
        sqlite_symbols = len(historical_data) if historical_data else 0
        
        if sqlite_symbols < MIN_SYMBOLS_THRESHOLD:
            # SQLite doesn't have enough data, fall back to pickle
            self.logger.debug(f"SQLite has only {sqlite_symbols} symbols (need {MIN_SYMBOLS_THRESHOLD}+), falling back to pickle")
            self._load_pickle_data()
            historical_data = self._pickle_data
            
            if historical_data and len(historical_data) >= MIN_SYMBOLS_THRESHOLD:
                self.logger.info(f"Loaded {len(historical_data)} symbols from pickle")
            else:
                # Try Turso database as last resort
                self.logger.debug("Trying Turso database for historical data")
                db_data = self._fetch_data_from_database(start_date, end_date)
                if db_data:
                    historical_data = self._convert_old_format_to_symbol_dataframe_format(db_data)
        else:
            self.logger.info(f"Loaded {sqlite_symbols} symbols from local SQLite")
        
        # Step 2: Get real-time data from InMemoryCandleStore (if available)
        realtime_data = None
        try:
            from pkbrokers.kite.inMemoryCandleStore import get_candle_store
            candle_store = get_candle_store()
            if candle_store.get_all_symbols():  # Only if store has data
                realtime_data = self._get_realtime_candle_data(interval='day')
        except Exception as e:
            self.logger.debug(f"InMemoryCandleStore not available: {e}")
        
        # Step 3: Merge historical with real-time (if real-time available)
        if historical_data:
            if realtime_data:
                self.pickle_data = self._merge_realtime_data_with_historical(
                    historical_data, realtime_data
                )
                self.logger.info(f"Merged {len(realtime_data)} real-time updates")
            else:
                self.pickle_data = historical_data
            
            return True
        
        return False

    @property
    def pickle_data(self):
        if not self._pickle_data_loaded:
            self._load_pickle_data()
            self._pickle_data_loaded = True
        return self._pickle_data

    @pickle_data.setter
    def pickle_data(self, value):
        self._pickle_data = value
        self._pickle_data_loaded = True

    def _is_symbol_dataframe_format(self, data: Any) -> bool:
        """
        Check if data is in symbol-indexed DataFrame-compatible format.
        
        Args:
            data: Data to check
            
        Returns:
            bool: True if data is in symbol-indexed DataFrame format, False otherwise
        """
        if not isinstance(data, dict) or not data:
            return False
        
        # Check if it's symbol-indexed format
        for symbol, symbol_data in data.items():
            if not isinstance(symbol_data, dict):
                return False
            
            # Check for required keys
            if not all(key in symbol_data for key in ['data', 'columns', 'index']):
                return False
            
            # Validate data types and structure
            if not isinstance(symbol_data['data'], list):
                return False
            
            if not isinstance(symbol_data['columns'], list):
                return False
            
            if not isinstance(symbol_data['index'], list):
                return False
            
            # Check if columns match expected OHLCV format
            expected_columns = ['open', 'high', 'low', 'close', 'volume']
            if symbol_data['columns'] != expected_columns:
                return False
            
            # Check if data rows match index length
            if len(symbol_data['data']) != len(symbol_data['index']):
                return False
            
            # Check if each data row has correct number of columns
            for row in symbol_data['data']:
                if not isinstance(row, list) or len(row) != len(expected_columns):
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
        if not isinstance(data, dict) or not data:
            return False
        
        # Check first few symbols to determine format
        sample_symbols = list(data.keys())[:3]  # Check first 3 symbols
        
        for symbol in sample_symbols:
            symbol_data = data[symbol]
            
            if not isinstance(symbol_data, dict):
                return False
            
            # Check if it's old format (nested dictionaries)
            sample_dates = list(symbol_data.keys())[:2] if symbol_data else []
            
            for date_key in sample_dates:
                ohlcv_data = symbol_data[date_key]
                
                if not isinstance(ohlcv_data, dict):
                    return False
                
                # Check for OHLCV keys (old format)
                if not all(key in ohlcv_data for key in ['open', 'high', 'low', 'close', 'volume']):
                    return False
                
                # Additional check: values should be numeric
                for key in ['open', 'high', 'low', 'close', 'volume']:
                    value = ohlcv_data.get(key)
                    if value is not None and not isinstance(value, (int, float)):
                        return False
        
        return True

    def _is_hybrid_format(self, data: Any) -> bool:
        """
        Check if data is in hybrid format (previous implementation).
        This method can be removed if hybrid format is no longer used.
        """
        return (isinstance(data, dict) and 
                "symbol_data" in data and 
                "dataframe_format" in data and
                "metadata" in data and
                isinstance(data["symbol_data"], dict))

    def _is_legacy_dataframe_format(self, data: Any) -> bool:
        """
        Check if data is in the legacy single DataFrame format.
        This would be the format where the entire pickle is one big DataFrame structure.
        """
        return (isinstance(data, dict) and 
                "data" in data and 
                "columns" in data and 
                "index" in data and
                not any(key in data for key in ['symbol_data', 'metadata']))  # Not hybrid format

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

    def _detect_data_format(self, data: Any) -> str:
        """
        Detect the format of the loaded data.
        
        Returns:
            str: Format type - 'symbol_dataframe', 'old', 'hybrid', 'legacy_dataframe', or 'unknown'
        """
        if self._is_symbol_dataframe_format(data):
            return 'symbol_dataframe'
        elif self._is_old_format(data):
            return 'old'
        elif self._is_hybrid_format(data):
            return 'hybrid'
        elif self._is_legacy_dataframe_format(data):
            return 'legacy_dataframe'
        else:
            return 'unknown'

    def _convert_legacy_dataframe_to_symbol_format(self, legacy_data: Dict) -> Dict[str, Any]:
        """
        Convert legacy single DataFrame format to symbol-indexed format.
        
        Args:
            legacy_data: Dictionary with 'data', 'columns', 'index' keys
            
        Returns:
            Dict: Symbol-indexed DataFrame format
        """
        if not legacy_data or not all(key in legacy_data for key in ['data', 'columns', 'index']):
            return {}
        
        symbol_dataframe_format = {}
        
        # Extract symbols from MultiIndex columns if present
        if isinstance(legacy_data['columns'], pd.MultiIndex):
            symbols = legacy_data['columns'].get_level_values(0).unique()
        else:
            # Assume columns are in order: [symbol1_open, symbol1_high, ..., symbol2_open, ...]
            symbols = set()
            for col in legacy_data['columns']:
                if '_' in col:
                    symbols.add(col.split('_')[0])
        
        for symbol in symbols:
            # Extract data for this symbol
            symbol_columns = [col for col in legacy_data['columns'] if str(col).startswith(f"{symbol}_")]
            if not symbol_columns:
                continue
            
            # Get column indices
            col_indices = [legacy_data['columns'].index(col) for col in symbol_columns]
            
            # Extract data rows
            symbol_data = []
            for row in legacy_data['data']:
                symbol_row = [row[i] for i in col_indices]
                symbol_data.append(symbol_row)
            
            # Clean column names (remove symbol prefix)
            clean_columns = [col.split('_', 1)[1] for col in symbol_columns]
            
            symbol_dataframe_format[symbol] = {
                'data': symbol_data,
                'columns': clean_columns,
                'index': legacy_data['index']
            }
        
        return symbol_dataframe_format

    def _convert_old_format_to_symbol_dataframe_format(self, old_format_data: Dict) -> Dict[str, Any]:
        """Optimized conversion from old format to symbol-indexed DataFrame format."""
        if not old_format_data:
            return {}

        symbol_dataframe_format = {}
        columns = ['open', 'high', 'low', 'close', 'volume']
        
        for symbol, symbol_data in old_format_data.items():
            # Pre-allocate lists
            timestamps = []
            data_rows = []
            
            for timestamp_str, ohlcv in symbol_data.items():
                # Use direct string manipulation for faster timestamp normalization
                if isinstance(timestamp_str, str):
                    if len(timestamp_str) == 10:  # Date only format "YYYY-MM-DD"
                        normalized_ts = f"{timestamp_str}T00:00:00+05:30"
                    else:
                        normalized_ts = timestamp_str
                else:
                    normalized_ts = str(timestamp_str)
                
                timestamps.append(normalized_ts)
                data_rows.append([
                    ohlcv.get('open'),
                    ohlcv.get('high'), 
                    ohlcv.get('low'),
                    ohlcv.get('close'),
                    ohlcv.get('volume', 0)
                ])
            
            # Sort using zip for better performance
            combined = list(zip(timestamps, data_rows))
            combined.sort(key=lambda x: x[0])
            
            sorted_timestamps, sorted_data = zip(*combined) if combined else ([], [])
            
            symbol_dataframe_format[symbol] = {
                'data': list(sorted_data),
                'columns': columns,
                'index': list(sorted_timestamps)
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
        from pkbrokers.kite.threadSafeDatabase import DEFAULT_DB_PATH
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
        Load pickle data from local file with improved format detection and conversion.
        """
        try:
            with open(self.local_pickle_path, "rb") as f:
                loaded_data = pickle.load(f)
            
            format_type = self._detect_data_format(loaded_data)
            
            if format_type == 'symbol_dataframe':
                self.pickle_data = loaded_data
                self.logger.info("Loaded data in symbol-indexed DataFrame format from local file")
                
            elif format_type == 'old':
                self.logger.info("Converting old format to symbol-indexed DataFrame format")
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(loaded_data)
                self._save_pickle_file()
                
            elif format_type == 'legacy_dataframe':
                self.logger.info("Converting legacy DataFrame format to symbol-indexed format")
                self.pickle_data = self._convert_legacy_dataframe_to_symbol_format(loaded_data)
                self._save_pickle_file()
                
            elif format_type == 'hybrid':
                self.logger.info("Converting hybrid format to symbol-indexed DataFrame format")
                # Extract symbol_data from hybrid format and convert
                old_format = self._convert_hybrid_to_old_format(loaded_data)
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(old_format)
                self._save_pickle_file()
                
            else:
                self.logger.error("Unknown data format in local pickle file")
                return None
            
            return self.pickle_data
            
        except Exception as e:
            self.logger.error(f"Failed to load local pickle file: {e}")
            return None

    def _create_dataframe_format(self, symbol_data: Dict, all_timestamps: set) -> Dict:
        """
        Create DataFrame-compatible format from symbol data.
        """
        sorted_timestamps = sorted(all_timestamps)
        sorted_symbols = sorted(symbol_data.keys())
        
        # Create MultiIndex columns
        columns = pd.MultiIndex.from_product(
            [sorted_symbols, ['open', 'high', 'low', 'close', 'volume']],
            names=['symbol', 'field']
        )
        
        data = []
        for timestamp in sorted_timestamps:
            row = []
            for symbol in sorted_symbols:
                ohlcv = symbol_data.get(symbol, {}).get(timestamp)
                if ohlcv:
                    row.extend([
                        ohlcv.get('open'),
                        ohlcv.get('high'),
                        ohlcv.get('low'),
                        ohlcv.get('close'),
                        ohlcv.get('volume')
                    ])
                else:
                    row.extend([None, None, None, None, None])
            data.append(row)
        
        return {
            "data": data,
            "columns": columns,
            "index": sorted_timestamps
        }

    def _create_metadata(self, symbol_data: Dict, all_timestamps: set) -> Dict:
        """
        Create metadata for the hybrid format.
        """
        kolkata_tz = pytz.timezone("Asia/Kolkata")
        return {
            "version": "1.0",
            "created_at": datetime.now(kolkata_tz).isoformat(),
            "symbol_count": len(symbol_data),
            "timestamp_count": len(all_timestamps),
            "timezone": "Asia/Kolkata",
            "data_format": "hybrid"
        }

    def _create_empty_hybrid_format(self) -> Dict:
        """
        Create an empty hybrid format structure.
        """
        kolkata_tz = pytz.timezone("Asia/Kolkata")
        return {
            "symbol_data": {},
            "dataframe_format": {"data": [], "columns": [], "index": []},
            "metadata": {
                "version": "1.0",
                "created_at": datetime.now(kolkata_tz).isoformat(),
                "symbol_count": 0,
                "timestamp_count": 0,
                "timezone": "Asia/Kolkata",
                "data_format": "hybrid"
            }
        }

    def _convert_old_to_hybrid_format(self, old_format_data: Dict) -> Dict:
        """
        Convert old format to hybrid format (for backward compatibility if needed).
        
        Args:
            old_format_data: Dictionary in old format {symbol: {date: {ohlcv_data}}}
            
        Returns:
            Dict: Hybrid format dictionary
        """
        if not old_format_data:
            return self._create_empty_hybrid_format()
        
        try:
            # Preserve original symbol data structure
            symbol_data = {}
            all_timestamps = set()
            
            for symbol, symbol_data_old in old_format_data.items():
                symbol_data[symbol] = {}
                for timestamp, ohlcv in symbol_data_old.items():
                    normalized_ts = self._normalize_timestamp(timestamp)
                    if normalized_ts:
                        symbol_data[symbol][normalized_ts] = ohlcv.copy()
                        all_timestamps.add(normalized_ts)
            
            # Create DataFrame-compatible format
            dataframe_format = self._create_dataframe_format(symbol_data, all_timestamps)
            
            # Create metadata
            metadata = self._create_metadata(symbol_data, all_timestamps)
            
            return {
                "symbol_data": symbol_data,
                "dataframe_format": dataframe_format,
                "metadata": metadata
            }
            
        except Exception as e:
            self.logger.error(f"Error converting to hybrid format: {e}")
            return self._create_empty_hybrid_format()
        
    def _convert_hybrid_to_old_format(self, hybrid_data: Dict) -> Dict:
        """
        Convert hybrid format data to old format {symbol: {date: {ohlcv_data}}}.
        
        Args:
            hybrid_data: Dictionary in hybrid format with 'symbol_data', 'dataframe_format', 'metadata'
            
        Returns:
            Dict: Old format dictionary {symbol: {date: {ohlcv_data}}}
        """
        if not hybrid_data or not isinstance(hybrid_data, dict):
            return {}
        
        # If symbol_data is available in hybrid format, use it directly
        if 'symbol_data' in hybrid_data and isinstance(hybrid_data['symbol_data'], dict):
            old_format_data = {}
            
            for symbol, symbol_data in hybrid_data['symbol_data'].items():
                old_format_data[symbol] = {}
                for timestamp, ohlcv_data in symbol_data.items():
                    # Extract only OHLCV values, excluding metadata
                    old_format_data[symbol][timestamp] = {
                        'open': ohlcv_data.get('open'),
                        'high': ohlcv_data.get('high'),
                        'low': ohlcv_data.get('low'),
                        'close': ohlcv_data.get('close'),
                        'volume': ohlcv_data.get('volume')
                    }
            
            return old_format_data
        
        # If only dataframe_format is available, reconstruct old format from it
        elif 'dataframe_format' in hybrid_data and isinstance(hybrid_data['dataframe_format'], dict):
            df_format = hybrid_data['dataframe_format']
            
            if not all(key in df_format for key in ['data', 'columns', 'index']):
                return {}
            
            old_format_data = {}
            
            # Process MultiIndex columns to extract symbol information
            if isinstance(df_format['columns'], pd.MultiIndex):
                # MultiIndex format: (symbol, field)
                for col_idx, (symbol, field) in enumerate(df_format['columns']):
                    if symbol not in old_format_data:
                        old_format_data[symbol] = {}
                    
                    for row_idx, timestamp in enumerate(df_format['index']):
                        if timestamp not in old_format_data[symbol]:
                            old_format_data[symbol][timestamp] = {}
                        
                        old_format_data[symbol][timestamp][field] = df_format['data'][row_idx][col_idx]
            
            else:
                # Flat columns format: assume [symbol1_open, symbol1_high, ..., symbol2_open, ...]
                for col_idx, col_name in enumerate(df_format['columns']):
                    if '_' in col_name:
                        symbol, field = col_name.split('_', 1)
                        
                        if symbol not in old_format_data:
                            old_format_data[symbol] = {}
                        
                        for row_idx, timestamp in enumerate(df_format['index']):
                            if timestamp not in old_format_data[symbol]:
                                old_format_data[symbol][timestamp] = {}
                            
                            old_format_data[symbol][timestamp][field] = df_format['data'][row_idx][col_idx]
            
            return old_format_data
        
        else:
            self.logger.warning("Hybrid format data doesn't contain expected structure")
            return {}
        
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
            
            format_type = self._detect_data_format(loaded_data)
            
            if format_type == 'symbol_dataframe':
                self.pickle_data = loaded_data
                self.logger.info("Loaded data in symbol-indexed DataFrame format from local file")
                
            elif format_type == 'old':
                self.logger.info("Converting old format to symbol-indexed DataFrame format")
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(loaded_data)
                self._save_pickle_file()
                
            elif format_type == 'legacy_dataframe':
                self.logger.info("Converting legacy DataFrame format to symbol-indexed format")
                self.pickle_data = self._convert_legacy_dataframe_to_symbol_format(loaded_data)
                self._save_pickle_file()
                
            elif format_type == 'hybrid':
                self.logger.info("Converting hybrid format to symbol-indexed DataFrame format")
                # Extract symbol_data from hybrid format and convert
                old_format = self._convert_hybrid_to_old_format(loaded_data)
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(old_format)
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
            pickle.dump(self.pickle_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
        self.logger.info(f"Pickle file saved: {self.local_pickle_path}")

    def _get_max_date_from_pickle_data(self) -> Optional[datetime]:
        """
        Find the maximum/latest timestamp in the loaded data.
        Simple and safe handling of mixed string/datetime timestamps.
        """
        if not self.pickle_data:
            return None

        try:
            max_datetime = None
            
            for symbol_data in self.pickle_data.values():
                for timestamp_item in symbol_data['index']:
                    try:
                        # Convert to datetime if it's a string
                        if isinstance(timestamp_item, str):
                            dt = datetime.fromisoformat(timestamp_item)
                        else:
                            # Assume it's already a datetime object
                            dt = timestamp_item
                        
                        # Make timezone naive for consistent comparison
                        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                            dt = dt.replace(tzinfo=None)
                        
                        if max_datetime is None or dt > max_datetime:
                            max_datetime = dt
                            
                    except (ValueError, TypeError, AttributeError):
                        # Skip any invalid items
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
            trading_instruments = self._get_trading_intruments()

            if not trading_instruments:
                self.logger.info("No trading instruments found to fetch data")
                return None

            # Format dates
            start_date_str = self._format_date(start_date)
            end_date_str = self._format_date(datetime.now())

            # Fetch historical data
            historical_data = kite_history.get_multiple_instruments_history(
                instruments=trading_instruments,
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
        # Skip database access if previously blocked (quota exceeded)
        if self._db_blocked:
            self.logger.debug("Skipping database fetch - access is blocked (quota exceeded)")
            return {}
        
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
            error_str = str(e)
            if "BLOCKED" in error_str or "reads are blocked" in error_str.lower():
                self.logger.warning(
                    "Database access blocked - quota exceeded. "
                    "Falling back to pickle/GitHub data. "
                    "Consider upgrading your Turso plan or using the scalable architecture."
                )
                self._db_blocked = True
            else:
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
        """Optimized ticks.json processing."""
        if not self.ticks_json_path.exists():
            return None

        try:
            with open(self.ticks_json_path, "r") as f:
                ticks_data = json.load(f)

            processed_data = {}
            timezone = pytz.timezone("Asia/Kolkata")
            
            for instrument_data in ticks_data.values():
                tradingsymbol = instrument_data.get("trading_symbol")
                ohlcv = instrument_data.get("ohlcv", {})
                timestamp = ohlcv.get("timestamp")
                
                if not tradingsymbol or not timestamp:
                    continue
                
                # Fast timestamp processing
                try:
                    if isinstance(timestamp, str):
                        if "+" not in timestamp:
                            timestamp_str = f"{timestamp}+05:30"
                        else:
                            timestamp_str = timestamp
                    else:
                        # Assume it's a timestamp number
                        dt = datetime.fromtimestamp(timestamp, tz=timezone)
                        timestamp_str = dt.isoformat()
                    
                    # Initialize symbol data if not exists
                    if tradingsymbol not in processed_data:
                        processed_data[tradingsymbol] = {}
                    
                    processed_data[tradingsymbol][timestamp_str] = {
                        "open": ohlcv.get("open"),
                        "high": ohlcv.get("high"),
                        "low": ohlcv.get("low"), 
                        "close": ohlcv.get("close"),
                        "volume": ohlcv.get("volume", 0)
                    }
                    
                except (ValueError, TypeError):
                    continue

            return processed_data

        except Exception as e:
            self.logger.error(f"Error processing ticks.json: {e}")
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

    def _get_missing_tradingsymbols(self) -> List[str]:
        saved_symbols = []
        if self.pickle_data:
            saved_symbols = list(self.pickle_data.keys())
        db_symbols = self._get_trading_intruments_from_db(column="tradingsymbol")
        return list(set(db_symbols) - set(saved_symbols))
    
    def _get_trading_intruments(self) -> List[int]:
        """
        Retrieve list of trading symbols from available data sources.

        Returns:
            List[int]: List of trading instruments
        """
        # if self.pickle_data:
        #     return list(self.pickle_data.keys())
        # else:
        return self._get_trading_intruments_from_db()

    def _get_trading_intruments_from_db(self, column="instrument_token") -> List[int]:
        """
        Fetch distinct trading instruments from instruments database table.

        Returns:
            List[int]: List of unique trading instruments from database
        """
        if not self._connect_to_database():
            return []

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(f"SELECT DISTINCT {column} FROM instruments")
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
                                if "+" not in timestamp:
                                    timestamp = f"{timestamp}+05:30"
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

    def _optimize_memory_usage(self):
        """Optimize memory usage of pickle data."""
        for symbol, symbol_data in self.pickle_data.items():
            # Convert to numpy arrays for memory efficiency
            if isinstance(symbol_data['data'], list):
                symbol_data['data'] = np.array(symbol_data['data'], dtype=np.float32)
            
            # Convert timestamps to datetime64 for efficiency
            if isinstance(symbol_data['index'], list):
                symbol_data['index'] = pd.to_datetime(symbol_data['index']).values
                
    def _update_pickle_file_batch(self, new_data_batch: List[Dict]):
        """Process multiple updates in batch."""
        if not new_data_batch:
            return
            
        # Merge all new data first
        merged_new_data = {}
        for new_data in new_data_batch:
            for symbol, symbol_data in new_data.items():
                if symbol not in merged_new_data:
                    merged_new_data[symbol] = symbol_data
                else:
                    merged_new_data[symbol].update(symbol_data)
        
        # Then update pickle once
        self._update_pickle_file(merged_new_data)
        
    def _update_pickle_file(self, new_data: Dict):
        """Optimized pickle file update with efficient merging."""
        # Convert new data to symbol-indexed DataFrame format
        new_data_symbol_format = self._convert_old_format_to_symbol_dataframe_format(new_data)
        
        if not self.pickle_data:
            self.pickle_data = new_data_symbol_format
            self._save_pickle_file()
            return

        # Process each symbol in new data
        for symbol, new_symbol_data in new_data_symbol_format.items():
            new_timestamps = new_symbol_data['index']
            new_data_values = new_symbol_data['data']
            
            if symbol in self.pickle_data:
                # Get existing data
                existing_data = self.pickle_data[symbol]
                existing_timestamps = existing_data['index']
                existing_values = existing_data['data']
                
                # Create mapping for fast lookup
                existing_date_map = {}
                for i, ts in enumerate(existing_timestamps):
                    if isinstance(ts, str):
                        date_key = ts.split('T')[0] if 'T' in ts else ts
                    else:
                        date_key = str(ts).split()[0]
                    existing_date_map[date_key] = (ts, existing_values[i])
                
                # Update with new data
                for i, new_ts in enumerate(new_timestamps):
                    if isinstance(new_ts, str):
                        date_key = new_ts.split('T')[0] if 'T' in new_ts else new_ts
                    else:
                        date_key = str(new_ts).split()[0]
                    
                    existing_date_map[date_key] = (new_ts, new_data_values[i])
                
                # Convert back to sorted lists
                all_entries = list(existing_date_map.values())
                all_entries.sort(key=lambda x: x[0])  # Sort by timestamp
                
                self.pickle_data[symbol] = {
                    'data': [data for _, data in all_entries],
                    'columns': existing_data['columns'],
                    'index': [ts for ts, _ in all_entries]
                }
            else:
                # New symbol
                self.pickle_data[symbol] = new_symbol_data

        self._save_pickle_file()

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

    def _load_pickle_data(self):
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

    def execute(self, fetch_kite=False, skip_db=False) -> bool:
        """
        Main execution method that orchestrates the complete data synchronization process.

        During market hours:
        - Prioritizes SQLite database for historical data
        - Merges with real-time aggregated candles from InMemoryCandleStore
        
        Outside market hours:
        - Uses pickle files as primary data source
        - Falls back to databases if needed

        Returns:
            bool: True if data was successfully loaded/created, False otherwise
        """
        self.logger.info("Starting data synchronization process...")

        # Check if we're in market hours
        try:
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            is_market_hours = PKDateUtilities.is_extended_market_hours()
        except Exception as e:
            self.logger.debug(f"Error checking market hours: {e}")
            is_market_hours = False
        
        # During market hours, use SQLite + real-time candles
        if is_market_hours:
            self.logger.info("Market is open - using SQLite + real-time candle data")
            if self._load_market_hours_data():
                # Continue to also try ticks download for latest data
                pass
            else:
                # Fallback to normal loading if market hours loading fails
                self.logger.debug("Market hours loading failed, falling back to normal flow")
                self._load_pickle_data()
        else:
            # Outside market hours, use pickle-first approach
            self._load_pickle_data()

        # Step 2: If no data loaded, fetch full year from database
        if not self.pickle_data:
            self.logger.info("Fetching full year data from database...")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            historical_data = self._fetch_data_from_database(start_date, end_date)

            if historical_data:
                self.pickle_data = self._convert_old_format_to_symbol_dataframe_format(historical_data)
                self._save_pickle_file()
                self.logger.debug("Initial pickle file created from database data")
            else:
                # Fallback: Try local SQLite database
                self.logger.debug("Trying local SQLite database as fallback...")
                local_data = self._fetch_data_from_local_sqlite(start_date, end_date)
                if local_data:
                    self.pickle_data = local_data
                    self._save_pickle_file()
                    self.logger.debug("Data loaded from local SQLite database")
                else:
                    self.logger.debug("No data available from any source")
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

            # Try database next
            if not incremental_data:
                from PKDevTools.classes.PKDateUtilities import PKDateUtilities
                if not PKDateUtilities.is_extended_market_hours() and not skip_db:
                    db_data = self._fetch_data_from_database(start_datetime, datetime.now())
                    if db_data:
                        incremental_data.update(db_data)
                        self.logger.debug(f"Added {len(db_data)} symbols from database")
            
            # Fallback: Try local SQLite database if Turso blocked
            if not incremental_data and self._db_blocked:
                local_data = self._fetch_data_from_local_sqlite(start_datetime, datetime.now())
                if local_data:
                    incremental_data.update(local_data)
                    self.logger.debug(f"Added {len(local_data)} symbols from local SQLite")

            # Update pickle with incremental data
            if incremental_data:
                self._update_pickle_file(incremental_data)
                self.logger.debug(
                    f"Updated with {len(incremental_data)} incremental records"
                )

        # Step 4: Try to get real-time candle updates (only if InMemoryCandleStore has data)
        # Skip ticks orchestration if we already have sufficient data loaded
        has_sufficient_data = self.pickle_data and len(self.pickle_data) >= 100
        
        if is_market_hours and has_sufficient_data:
            # During market hours, try to get real-time aggregated candles
            try:
                from pkbrokers.kite.inMemoryCandleStore import get_candle_store
                candle_store = get_candle_store()
                if candle_store.get_all_symbols():  # Only if store has been populated
                    realtime_data = self._get_realtime_candle_data(interval='day')
                    if realtime_data:
                        self.pickle_data = self._merge_realtime_data_with_historical(
                            self.pickle_data or {}, realtime_data
                        )
                        self.logger.debug(f"Merged real-time data for {len(realtime_data)} symbols")
                        self._update_local_sqlite_from_ticks()
            except Exception as e:
                self.logger.debug(f"Real-time candle update skipped: {e}")
        
        # Only try ticks.json if it already exists locally (don't trigger auth-required download)
        if self.ticks_json_path.exists():
            ticks_data = self._load_and_process_ticks_json()
            if ticks_data:
                if is_market_hours:
                    self.pickle_data = self._merge_realtime_data_with_historical(
                        self.pickle_data or {}, ticks_data
                    )
                    self.logger.debug(f"Merged ticks.json data for {len(ticks_data)} symbols")
                else:
                    self._update_pickle_file(ticks_data)
                    self.logger.debug(f"Updated with {len(ticks_data)} records from ticks.json")
                self._update_local_sqlite_from_ticks()

        if fetch_kite:
            # Try Kite API first
            kite_data = self._get_recent_data_from_kite(start_datetime)
            if kite_data:
                incremental_data.update(kite_data)
                self.logger.debug(f"Added {len(kite_data)} symbols from Kite API")
        else:
            try:
                missing_symbols = self._get_missing_tradingsymbols()
                if len(missing_symbols) > 0:
                    self.logger.error(f"Symbols found missing from pkl file but present in DB: {missing_symbols}. You may wish to enable 'fetch_kite' in instrumentDataManager.execute().")
            except Exception as e:
                self.logger.error(f"Error while trying to find missing symbols:{e}")
        self.logger.debug("Data synchronization process completed")
        return self.pickle_data is not None
