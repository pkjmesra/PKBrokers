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

DataSharingManager - Manages data sharing between PKTickBot instances
=====================================================================

This module handles:
- Requesting data (pkl files, SQLite DBs) from running bot instances
- Downloading fallback data from GitHub actions-data-download branch
- Committing updated pkl files when market closes
- Holiday and market hours awareness

"""

import gzip
import json
import logging
import os
import pickle
import shutil
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests

from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger

# Constants
KOLKATA_TZ = pytz.timezone("Asia/Kolkata")
DEFAULT_PATH = Archiver.get_user_data_dir()

# GitHub URLs for fallback data
PKSCREENER_RAW_BASE = "https://raw.githubusercontent.com/pkjmesra/PKScreener"
ACTIONS_DATA_BRANCH = "actions-data-download"

# File names
DAILY_PKL_FILE = "daily_candles.pkl"
INTRADAY_PKL_FILE = "intraday_1m_candles.pkl"
DAILY_DB_FILE = "daily_candles.db"
INTRADAY_DB_FILE = "intraday_candles.db"

# Market hours (IST)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

# Holiday cache
_holiday_cache: Optional[Dict[str, List[str]]] = None
_holiday_cache_date: Optional[str] = None


class DataSharingManager:
    """
    Manages data sharing between PKTickBot instances.
    
    Features:
    - Request pkl/db files from running bot instance via Telegram
    - Download fallback data from GitHub when no running instance available
    - Detect market close and auto-commit pkl files
    - Track trading holidays from NSE holiday list
    """
    
    def __init__(self, data_dir: str = None):
        """
        Initialize the DataSharingManager.
        
        Args:
            data_dir: Directory for storing data files (defaults to user data dir)
        """
        self.data_dir = data_dir or DEFAULT_PATH
        self.logger = default_logger()
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Track state
        self.data_received_from_instance = False
        self.last_commit_time = None
        
    def get_daily_pkl_path(self) -> str:
        """Get path to daily candle pkl file."""
        return os.path.join(self.data_dir, DAILY_PKL_FILE)
    
    def get_intraday_pkl_path(self) -> str:
        """Get path to intraday 1-min candle pkl file."""
        return os.path.join(self.data_dir, INTRADAY_PKL_FILE)
    
    def get_date_suffixed_pkl_path(self, base_name: str, date: datetime = None) -> str:
        """
        Get path to date-suffixed pkl file.
        
        Args:
            base_name: Base file name (e.g., 'stock_data' or 'intraday_stock_data')
            date: Date for suffix (defaults to today)
            
        Returns:
            Path like stock_data_26122025.pkl
        """
        if date is None:
            date = datetime.now(KOLKATA_TZ)
        date_str = date.strftime("%d%m%Y")
        return os.path.join(self.data_dir, f"{base_name}_{date_str}.pkl")
    
    def is_market_open(self) -> bool:
        """Check if market is currently open."""
        now = datetime.now(KOLKATA_TZ)
        
        # Check if today is a trading day
        if not self.is_trading_day(now):
            return False
        
        # Check market hours
        market_open = now.replace(
            hour=MARKET_OPEN_HOUR,
            minute=MARKET_OPEN_MINUTE,
            second=0,
            microsecond=0
        )
        market_close = now.replace(
            hour=MARKET_CLOSE_HOUR,
            minute=MARKET_CLOSE_MINUTE,
            second=0,
            microsecond=0
        )
        
        return market_open <= now <= market_close
    
    def is_trading_day(self, date: datetime = None) -> bool:
        """
        Check if the given date is a trading day.
        
        Args:
            date: Date to check (defaults to today)
            
        Returns:
            True if it's a trading day (weekday and not a holiday)
        """
        if date is None:
            date = datetime.now(KOLKATA_TZ)
        
        # Check if weekend
        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Check if holiday
        if self.is_holiday(date):
            return False
        
        return True
    
    def is_holiday(self, date: datetime = None) -> bool:
        """
        Check if the given date is a market holiday.
        
        Args:
            date: Date to check (defaults to today)
            
        Returns:
            True if it's a holiday
        """
        if date is None:
            date = datetime.now(KOLKATA_TZ)
        
        holidays = self._get_holidays()
        if not holidays:
            return False
        
        # Format date to match holiday JSON format: DD-MMM-YYYY
        date_str = date.strftime("%d-%b-%Y")
        
        return date_str in holidays
    
    def _get_holidays(self) -> List[str]:
        """
        Get list of trading holidays for current year.
        
        Returns:
            List of holiday dates in DD-MMM-YYYY format
        """
        global _holiday_cache, _holiday_cache_date
        
        today = datetime.now(KOLKATA_TZ).strftime("%Y-%m-%d")
        
        # Return cached if available and recent
        if _holiday_cache is not None and _holiday_cache_date == today:
            return _holiday_cache.get("dates", [])
        
        try:
            # Download holidays JSON
            url = f"{PKSCREENER_RAW_BASE}/main/.github/dependencies/nse-holidays.json"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            holidays_data = response.json()
            
            # Extract dates from CM2025 key (or CM{year} for current year)
            current_year = datetime.now(KOLKATA_TZ).year
            cm_key = f"CM{current_year}"
            
            trading_holidays = holidays_data.get(cm_key, [])
            holiday_dates = [h.get("tradingDate", "") for h in trading_holidays if h.get("tradingDate")]
            
            # Cache the result
            _holiday_cache = {"dates": holiday_dates}
            _holiday_cache_date = today
            
            self.logger.info(f"Loaded {len(holiday_dates)} holidays for {current_year}")
            return holiday_dates
            
        except Exception as e:
            self.logger.error(f"Error fetching holidays: {e}")
            return []
    
    def is_market_about_to_close(self, minutes_before: int = 5) -> bool:
        """
        Check if market is about to close within given minutes.
        
        Args:
            minutes_before: Minutes before market close to trigger
            
        Returns:
            True if within minutes_before of market close
        """
        now = datetime.now(KOLKATA_TZ)
        
        if not self.is_trading_day(now):
            return False
        
        market_close = now.replace(
            hour=MARKET_CLOSE_HOUR,
            minute=MARKET_CLOSE_MINUTE,
            second=0,
            microsecond=0
        )
        
        time_to_close = (market_close - now).total_seconds() / 60
        
        return 0 <= time_to_close <= minutes_before
    
    def download_from_github(self, file_type: str = "daily") -> Tuple[bool, Optional[str]]:
        """
        Download pkl file from GitHub actions-data-download branch.
        
        Args:
            file_type: "daily" or "intraday"
            
        Returns:
            Tuple of (success, file_path)
        """
        try:
            today = datetime.now(KOLKATA_TZ)
            
            # Try date-specific file first, then generic
            if file_type == "daily":
                date_file = f"stock_data_{today.strftime('%d%m%Y')}.pkl"
                generic_file = "stock_data_*.pkl"
                output_path = self.get_daily_pkl_path()
            else:
                date_file = f"intraday_stock_data_{today.strftime('%d%m%Y')}.pkl"
                generic_file = "intraday_stock_data_*.pkl"
                output_path = self.get_intraday_pkl_path()
            
            # URLs to try
            urls_to_try = [
                f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/actions-data-download/{date_file}",
                f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/{date_file}",
            ]
            
            # Try each URL
            for url in urls_to_try:
                try:
                    self.logger.info(f"Trying to download from: {url}")
                    response = requests.get(url, timeout=60)
                    
                    if response.status_code == 200:
                        with open(output_path, 'wb') as f:
                            f.write(response.content)
                        
                        self.logger.info(f"Downloaded {file_type} pkl from GitHub: {output_path}")
                        return True, output_path
                        
                except requests.RequestException as e:
                    self.logger.debug(f"Failed to download from {url}: {e}")
                    continue
            
            self.logger.warning(f"Could not download {file_type} pkl from GitHub")
            return False, None
            
        except Exception as e:
            self.logger.error(f"Error downloading from GitHub: {e}")
            return False, None
    
    def export_daily_candles_to_pkl(self, candle_store) -> Tuple[bool, Optional[str]]:
        """
        Export daily candles from InMemoryCandleStore to pkl file.
        
        Args:
            candle_store: InMemoryCandleStore instance
            
        Returns:
            Tuple of (success, file_path)
        """
        try:
            import pandas as pd
            
            output_path = self.get_daily_pkl_path()
            data = {}
            
            with candle_store.lock:
                for token, instrument in candle_store.instruments.items():
                    symbol = candle_store.instrument_symbols.get(token, str(token))
                    
                    # Get all daily candles including current
                    day_candles = list(instrument.candles.get('day', []))
                    current_day = instrument.current_candle.get('day')
                    if current_day and current_day.tick_count > 0:
                        day_candles.append(current_day)
                    
                    if not day_candles:
                        continue
                    
                    # Convert to DataFrame
                    rows = []
                    for candle in day_candles:
                        dt = datetime.fromtimestamp(candle.timestamp, tz=KOLKATA_TZ)
                        rows.append({
                            'Date': dt,
                            'Open': candle.open,
                            'High': candle.high,
                            'Low': candle.low if candle.low != float('inf') else candle.open,
                            'Close': candle.close,
                            'Volume': candle.volume,
                        })
                    
                    if rows:
                        df = pd.DataFrame(rows)
                        df.set_index('Date', inplace=True)
                        data[symbol] = df
            
            if data:
                with open(output_path, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                
                self.logger.info(f"Exported {len(data)} instruments to {output_path}")
                return True, output_path
            else:
                self.logger.warning("No daily candle data to export")
                return False, None
                
        except Exception as e:
            self.logger.error(f"Error exporting daily candles: {e}")
            return False, None
    
    def export_intraday_candles_to_pkl(self, candle_store) -> Tuple[bool, Optional[str]]:
        """
        Export 1-minute candles from InMemoryCandleStore to pkl file.
        
        Args:
            candle_store: InMemoryCandleStore instance
            
        Returns:
            Tuple of (success, file_path)
        """
        try:
            import pandas as pd
            
            output_path = self.get_intraday_pkl_path()
            data = {}
            
            with candle_store.lock:
                for token, instrument in candle_store.instruments.items():
                    symbol = candle_store.instrument_symbols.get(token, str(token))
                    
                    # Get all 1-min candles including current
                    candles = list(instrument.candles.get('1m', []))
                    current = instrument.current_candle.get('1m')
                    if current and current.tick_count > 0:
                        candles.append(current)
                    
                    if not candles:
                        continue
                    
                    # Convert to DataFrame
                    rows = []
                    for candle in candles:
                        dt = datetime.fromtimestamp(candle.timestamp, tz=KOLKATA_TZ)
                        rows.append({
                            'Date': dt,
                            'Open': candle.open,
                            'High': candle.high,
                            'Low': candle.low if candle.low != float('inf') else candle.open,
                            'Close': candle.close,
                            'Volume': candle.volume,
                        })
                    
                    if rows:
                        df = pd.DataFrame(rows)
                        df.set_index('Date', inplace=True)
                        data[symbol] = df
            
            if data:
                with open(output_path, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                
                self.logger.info(f"Exported {len(data)} instruments to {output_path}")
                return True, output_path
            else:
                self.logger.warning("No intraday candle data to export")
                return False, None
                
        except Exception as e:
            self.logger.error(f"Error exporting intraday candles: {e}")
            return False, None
    
    def load_pkl_into_candle_store(self, pkl_path: str, candle_store, interval: str = 'day') -> int:
        """
        Load pkl file data into InMemoryCandleStore.
        
        Args:
            pkl_path: Path to pkl file
            candle_store: InMemoryCandleStore instance
            interval: Candle interval ('day' or '1m')
            
        Returns:
            Number of instruments loaded
        """
        try:
            if not os.path.exists(pkl_path):
                self.logger.warning(f"Pkl file not found: {pkl_path}")
                return 0
            
            with open(pkl_path, 'rb') as f:
                data = pickle.load(f)
            
            loaded = 0
            for symbol, df in data.items():
                if hasattr(df, 'iterrows'):
                    # It's a DataFrame
                    for idx, row in df.iterrows():
                        tick_data = {
                            'trading_symbol': symbol,
                            'last_price': row.get('Close', 0),
                            'day_volume': row.get('Volume', 0),
                            'exchange_timestamp': idx.timestamp() if hasattr(idx, 'timestamp') else datetime.now().timestamp(),
                        }
                        # Note: This will register the symbol but won't fully reconstruct historical candles
                        # It's primarily for initialization and symbol registration
                        candle_store.register_instrument(
                            candle_store.symbol_to_token.get(symbol, hash(symbol) % (10 ** 9)),
                            symbol
                        )
                    loaded += 1
            
            self.logger.info(f"Loaded {loaded} instruments from {pkl_path}")
            return loaded
            
        except Exception as e:
            self.logger.error(f"Error loading pkl file: {e}")
            return 0
    
    def zip_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
        """
        Create a zip file from the given file.
        
        Args:
            file_path: Path to file to zip
            
        Returns:
            Tuple of (success, zip_path)
        """
        try:
            if not os.path.exists(file_path):
                return False, None
            
            zip_path = file_path + ".zip"
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, os.path.basename(file_path))
            
            return True, zip_path
            
        except Exception as e:
            self.logger.error(f"Error zipping file: {e}")
            return False, None
    
    def commit_pkl_files(self, branch_name: str = "actions-data-download") -> bool:
        """
        Commit pkl files to GitHub repository.
        
        Args:
            branch_name: Branch to commit to
            
        Returns:
            True if commit was successful
        """
        try:
            from PKDevTools.classes.Committer import Committer
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            
            files_to_commit = []
            
            # Check for daily pkl
            daily_pkl = self.get_daily_pkl_path()
            if os.path.exists(daily_pkl):
                files_to_commit.append(daily_pkl)
            
            # Check for intraday pkl
            intraday_pkl = self.get_intraday_pkl_path()
            if os.path.exists(intraday_pkl):
                files_to_commit.append(intraday_pkl)
            
            if not files_to_commit:
                self.logger.warning("No pkl files to commit")
                return False
            
            for file_path in files_to_commit:
                try:
                    Committer.execOSCommand(f"git add {file_path} -f >/dev/null 2>&1")
                    commit_path = f"-A '{file_path}'"
                    
                    self.logger.info(f"Committing {os.path.basename(file_path)} to {branch_name}")
                    
                    Committer.commitTempOutcomes(
                        addPath=commit_path,
                        commitMessage=f"[{os.path.basename(file_path)}-{PKDateUtilities.currentDateTime()}]",
                        branchName=branch_name,
                        showStatus=True,
                        timeout=900,
                    )
                except Exception as e:
                    self.logger.error(f"Error committing {file_path}: {e}")
            
            self.last_commit_time = datetime.now(KOLKATA_TZ)
            self.logger.info("Pkl files committed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error committing pkl files: {e}")
            return False
    
    def should_commit(self) -> bool:
        """
        Determine if we should commit pkl files now.
        
        Returns:
            True if we should commit
        """
        # Commit if market is about to close (within 5 minutes)
        if self.is_market_about_to_close(minutes_before=5):
            return True
        
        # Commit if we just received data from another instance
        if self.data_received_from_instance:
            self.data_received_from_instance = False
            return True
        
        return False


# Singleton instance
_data_sharing_manager: Optional[DataSharingManager] = None


def get_data_sharing_manager() -> DataSharingManager:
    """Get the global DataSharingManager instance."""
    global _data_sharing_manager
    if _data_sharing_manager is None:
        _data_sharing_manager = DataSharingManager()
    return _data_sharing_manager


