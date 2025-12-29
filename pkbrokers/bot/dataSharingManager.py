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
    
    def download_from_github(self, file_type: str = "daily", validate_freshness: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Download pkl file from GitHub actions-data-download branch.
        
        Searches for pkl files in multiple locations and date formats:
        - actions-data-download/stock_data_DDMMYYYY.pkl
        - results/Data/stock_data_DDMMYYYY.pkl
        - Also tries recent dates going back up to 10 days
        
        Args:
            file_type: "daily" or "intraday"
            validate_freshness: If True, check if data is fresh and trigger history download if stale
            
        Returns:
            Tuple of (success, file_path)
        """
        try:
            from datetime import timedelta
            today = datetime.now(KOLKATA_TZ)
            
            if file_type == "daily":
                output_path = self.get_daily_pkl_path()
                file_prefix = "stock_data"
            else:
                output_path = self.get_intraday_pkl_path()
                file_prefix = "intraday_stock_data"
            
            # Build list of URLs to try - multiple dates and locations
            urls_to_try = []
            
            # Try last 10 days (to handle weekends/holidays)
            for days_ago in range(0, 10):
                check_date = today - timedelta(days=days_ago)
                date_str_full = check_date.strftime('%d%m%Y')  # e.g., 29122025
                # Short format without leading zero - compatible across platforms
                day_no_zero = str(int(check_date.strftime('%d')))
                date_str_short = f"{day_no_zero}{check_date.strftime('%m%y')}"  # e.g., 171225
                # Also try YYMMDD format (used by localCandleDatabase)
                date_str_yymmdd = check_date.strftime('%y%m%d')  # e.g., 251229
                
                # Try all date formats in both locations
                for date_str in [date_str_full, date_str_short, date_str_yymmdd]:
                    date_file = f"{file_prefix}_{date_str}.pkl"
                    
                    # Location 1: actions-data-download/actions-data-download/
                    urls_to_try.append(
                        f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/actions-data-download/{date_file}"
                    )
                    # Location 2: actions-data-download/results/Data/
                    urls_to_try.append(
                        f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/results/Data/{date_file}"
                    )
            
            # Also try generic names without date
            for generic_name in [f"{file_prefix}.pkl", "daily_candles.pkl", "intraday_1m_candles.pkl"]:
                urls_to_try.append(f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/actions-data-download/{generic_name}")
                urls_to_try.append(f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/results/Data/{generic_name}")
            
            # Try each URL
            for url in urls_to_try:
                try:
                    self.logger.debug(f"Trying to download from: {url}")
                    response = requests.get(url, timeout=60)
                    
                    if response.status_code == 200 and len(response.content) > 1000:
                        # Ensure we got actual pkl content, not an error page
                        with open(output_path, 'wb') as f:
                            f.write(response.content)
                        
                        # Verify it's a valid pkl file
                        try:
                            with open(output_path, 'rb') as f:
                                data = pickle.load(f)
                            if data and len(data) > 0:
                                self.logger.info(f"Downloaded {file_type} pkl from GitHub: {url} ({len(data)} instruments)")
                                
                                # Validate freshness and trigger history download if stale
                                if validate_freshness and file_type == "daily":
                                    is_fresh, data_date, missing_days = self.validate_pkl_freshness(output_path)
                                    if not is_fresh and missing_days > 0:
                                        self.logger.warning(f"Downloaded pkl is stale by {missing_days} trading days. Triggering history download...")
                                        self.trigger_history_download_workflow(missing_days)
                                
                                return True, output_path
                        except (pickle.UnpicklingError, EOFError) as e:
                            self.logger.debug(f"Invalid pkl file from {url}: {e}")
                            continue
                        
                except requests.RequestException as e:
                    self.logger.debug(f"Failed to download from {url}: {e}")
                    continue
            
            self.logger.warning(f"Could not download {file_type} pkl from GitHub after trying {len(urls_to_try)} URLs")
            return False, None
            
        except Exception as e:
            self.logger.error(f"Error downloading from GitHub: {e}")
            return False, None
    
    def validate_pkl_freshness(self, pkl_path: str) -> Tuple[bool, Optional[datetime], int]:
        """
        Validate if pkl file data is fresh (has data up to last trading date).
        
        Args:
            pkl_path: Path to pkl file
            
        Returns:
            Tuple of (is_fresh, latest_data_date, missing_trading_days)
        """
        try:
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            
            if not os.path.exists(pkl_path):
                return False, None, 0
            
            with open(pkl_path, 'rb') as f:
                data = pickle.load(f)
            
            if not data:
                return False, None, 0
            
            # Find the latest date across all stocks
            latest_date = None
            for symbol, df in data.items():
                if hasattr(df, 'index') and len(df.index) > 0:
                    stock_last_date = df.index[-1]
                    if hasattr(stock_last_date, 'date'):
                        stock_last_date = stock_last_date.date()
                    elif hasattr(stock_last_date, 'to_pydatetime'):
                        stock_last_date = stock_last_date.to_pydatetime().date()
                    
                    if latest_date is None or stock_last_date > latest_date:
                        latest_date = stock_last_date
            
            if latest_date is None:
                return False, None, 0
            
            # Get the last trading date using PKDateUtilities
            last_trading_date = PKDateUtilities.tradingDate()
            if hasattr(last_trading_date, 'date'):
                last_trading_date = last_trading_date.date()
            
            # Check if data is fresh
            if latest_date >= last_trading_date:
                self.logger.info(f"Pkl data is fresh. Latest date: {latest_date}, Last trading date: {last_trading_date}")
                return True, latest_date, 0
            
            # Calculate missing trading days
            missing_days = PKDateUtilities.trading_days_between(latest_date, last_trading_date)
            
            self.logger.warning(f"Pkl data is stale. Latest date: {latest_date}, Last trading date: {last_trading_date}, Missing {missing_days} trading days")
            return False, latest_date, missing_days
            
        except Exception as e:
            self.logger.error(f"Error validating pkl freshness: {e}")
            return False, None, 0
    
    def trigger_history_download_workflow(self, past_offset: int = 1) -> bool:
        """
        Trigger the w1-workflow-history-data-child.yml workflow to download missing OHLCV data.
        
        Args:
            past_offset: Number of days of historical data to fetch
            
        Returns:
            True if workflow was triggered successfully
        """
        try:
            import os
            
            github_token = os.environ.get('GITHUB_TOKEN') or os.environ.get('CI_PAT')
            if not github_token:
                self.logger.error("GITHUB_TOKEN or CI_PAT not found. Cannot trigger workflow.")
                return False
            
            # Trigger PKBrokers history workflow
            url = "https://api.github.com/repos/pkjmesra/PKBrokers/actions/workflows/w1-workflow-history-data-child.yml/dispatches"
            
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            
            payload = {
                "ref": "main",
                "inputs": {
                    "period": "day",
                    "pastoffset": str(past_offset),
                    "logLevel": "20"
                }
            }
            
            self.logger.info(f"Triggering history download workflow with past_offset={past_offset}")
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 204:
                self.logger.info("Successfully triggered history download workflow")
                return True
            else:
                self.logger.error(f"Failed to trigger workflow: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error triggering history download workflow: {e}")
            return False
    
    def ensure_data_freshness_and_commit(self, pkl_path: str = None) -> bool:
        """
        Ensure pkl data is fresh. If stale, trigger history download and commit updated data.
        
        This is the main entry point for the freshness check workflow.
        
        Args:
            pkl_path: Path to pkl file (defaults to daily pkl)
            
        Returns:
            True if data is fresh or was successfully updated
        """
        try:
            if pkl_path is None:
                pkl_path = self.get_daily_pkl_path()
            
            is_fresh, data_date, missing_days = self.validate_pkl_freshness(pkl_path)
            
            if is_fresh:
                self.logger.info("Data is already fresh")
                return True
            
            if missing_days > 0:
                self.logger.info(f"Data is stale by {missing_days} trading days. Triggering history download...")
                
                # Trigger history download workflow
                triggered = self.trigger_history_download_workflow(missing_days)
                
                if triggered:
                    self.logger.info("History download workflow triggered. Fresh data will be available after workflow completes.")
                    # Note: The workflow will handle committing the updated pkl file
                    return True
                else:
                    self.logger.warning("Failed to trigger history download workflow")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error ensuring data freshness: {e}")
            return False
    
    def export_daily_candles_to_pkl(self, candle_store, merge_with_historical: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Export daily candles from InMemoryCandleStore to pkl file.
        Merges with historical data from GitHub to create complete ~35MB+ pkl files.
        
        Args:
            candle_store: InMemoryCandleStore instance
            merge_with_historical: Whether to merge with historical pkl from GitHub
            
        Returns:
            Tuple of (success, file_path)
        """
        try:
            import pandas as pd
            
            output_path = self.get_daily_pkl_path()
            data = {}
            
            # First, try to load existing historical data from GitHub
            if merge_with_historical:
                try:
                    self.logger.info("Attempting to download historical pkl from GitHub for merge...")
                    success, historical_path = self.download_from_github(file_type="daily", validate_freshness=False)
                    if success and historical_path and os.path.exists(historical_path):
                        with open(historical_path, 'rb') as f:
                            historical_data = pickle.load(f)
                        
                        self.logger.info(f"Downloaded historical pkl with {len(historical_data)} instruments")
                        
                        # Convert historical data to proper format
                        for symbol, df_or_dict in historical_data.items():
                            if isinstance(df_or_dict, dict):
                                # Convert split dict format to DataFrame
                                if 'data' in df_or_dict and 'columns' in df_or_dict and 'index' in df_or_dict:
                                    df = pd.DataFrame(
                                        df_or_dict['data'],
                                        columns=df_or_dict['columns'],
                                        index=pd.to_datetime(df_or_dict['index'])
                                    )
                                    # Rename columns to standard format
                                    col_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}
                                    df.rename(columns=col_map, inplace=True)
                                    data[symbol] = df
                            elif hasattr(df_or_dict, 'index'):
                                data[symbol] = df_or_dict
                        
                        self.logger.info(f"Loaded {len(data)} instruments from historical pkl into merge buffer")
                    else:
                        self.logger.warning("Could not download historical pkl from GitHub - will export only today's candle data")
                except Exception as he:
                    self.logger.warning(f"Could not load historical data: {he}")
            
            # Now add today's candles from the candle store
            today_count = 0
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
                        new_df = pd.DataFrame(rows)
                        new_df.set_index('Date', inplace=True)
                        
                        # Merge with historical data if exists
                        if symbol in data:
                            existing_df = data[symbol]
                            # Combine and remove duplicates (keep latest)
                            combined = pd.concat([existing_df, new_df])
                            combined = combined[~combined.index.duplicated(keep='last')]
                            combined.sort_index(inplace=True)
                            data[symbol] = combined
                        else:
                            data[symbol] = new_df
                        today_count += 1
            
            if data:
                with open(output_path, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                
                file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
                self.logger.info(f"Exported {len(data)} instruments ({today_count} with today's data) to {output_path} ({file_size:.2f} MB)")
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
                
                file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
                self.logger.info(f"Exported {len(data)} intraday instruments to {output_path} ({file_size:.2f} MB)")
                return True, output_path
            else:
                self.logger.warning("No intraday candle data to export")
                return False, None
                
        except Exception as e:
            self.logger.error(f"Error exporting intraday candles: {e}")
            return False, None
    
    def convert_ticks_json_to_pkl(self, ticks_json_path: str = None) -> Tuple[bool, Optional[str]]:
        """
        Convert ticks.json file directly to intraday pkl file.
        
        This is useful when the candle store is empty but ticks.json has data
        (e.g., downloaded from GitHub or another running instance).
        
        Args:
            ticks_json_path: Path to ticks.json file. Defaults to data_dir/ticks.json
            
        Returns:
            Tuple of (success, output_pkl_path)
        """
        try:
            import json
            import pandas as pd
            
            if ticks_json_path is None:
                ticks_json_path = os.path.join(self.data_dir, "ticks.json")
            
            if not os.path.exists(ticks_json_path):
                self.logger.warning(f"ticks.json not found at: {ticks_json_path}")
                return False, None
            
            with open(ticks_json_path, 'r') as f:
                ticks_data = json.load(f)
            
            if not ticks_data:
                self.logger.warning("Empty ticks.json file")
                return False, None
            
            self.logger.info(f"Converting {len(ticks_data)} instruments from ticks.json to pkl")
            
            data = {}
            for token_str, tick_info in ticks_data.items():
                try:
                    symbol = tick_info.get('trading_symbol', str(token_str))
                    ohlcv = tick_info.get('ohlcv', {})
                    
                    if not ohlcv:
                        continue
                    
                    # Parse timestamp
                    timestamp_str = ohlcv.get('timestamp', '')
                    if timestamp_str:
                        try:
                            dt = pd.to_datetime(timestamp_str)
                        except:
                            dt = datetime.now(KOLKATA_TZ)
                    else:
                        dt = datetime.now(KOLKATA_TZ)
                    
                    # Create single-row DataFrame with OHLCV data
                    df = pd.DataFrame([{
                        'Date': dt,
                        'Open': float(ohlcv.get('open', 0)),
                        'High': float(ohlcv.get('high', 0)),
                        'Low': float(ohlcv.get('low', 0)),
                        'Close': float(ohlcv.get('close', 0)),
                        'Volume': int(ohlcv.get('volume', 0)),
                    }])
                    df.set_index('Date', inplace=True)
                    
                    if df['Close'].iloc[0] > 0:
                        data[symbol] = df
                        
                except Exception as e:
                    self.logger.debug(f"Error processing tick for {token_str}: {e}")
                    continue
            
            if data:
                output_path = self.get_intraday_pkl_path()
                with open(output_path, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                
                file_size = os.path.getsize(output_path) / (1024 * 1024)
                self.logger.info(f"Converted {len(data)} instruments from ticks.json to {output_path} ({file_size:.2f} MB)")
                
                # Also create dated copy
                today_suffix = datetime.now(KOLKATA_TZ).strftime('%d%m%Y')
                dated_path = os.path.join(self.data_dir, f"intraday_stock_data_{today_suffix}.pkl")
                with open(dated_path, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                self.logger.info(f"Also saved as: {dated_path}")
                
                return True, output_path
            else:
                self.logger.warning("No valid data in ticks.json to convert")
                return False, None
                
        except Exception as e:
            self.logger.error(f"Error converting ticks.json to pkl: {e}")
            return False, None
    
    def load_pkl_into_candle_store(self, pkl_path: str, candle_store, interval: str = 'day') -> int:
        """
        Load pkl file data into InMemoryCandleStore.
        
        This method loads historical candle data from pkl files and populates
        the candle store so that ticks can be properly aggregated on top of
        existing historical data.
        
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
            
            if not data:
                self.logger.warning(f"Empty pkl file: {pkl_path}")
                return 0
            
            loaded = 0
            total_candles = 0
            
            for symbol, df_or_dict in data.items():
                try:
                    # Convert dict to DataFrame if needed
                    if isinstance(df_or_dict, dict):
                        import pandas as pd
                        if 'data' in df_or_dict and 'columns' in df_or_dict:
                            df = pd.DataFrame(df_or_dict['data'], columns=df_or_dict['columns'])
                            if 'index' in df_or_dict:
                                df.index = df_or_dict['index']
                        else:
                            continue
                    else:
                        df = df_or_dict
                    
                    if not hasattr(df, 'iterrows') or len(df) == 0:
                        continue
                    
                    # Generate a token for this symbol (use hash if not in lookup)
                    token = candle_store.symbol_to_token.get(symbol, hash(symbol) % (10 ** 9))
                    
                    # Register the instrument
                    candle_store.register_instrument(token, symbol)
                    
                    # Process each row as a simulated tick to build candles
                    for idx, row in df.iterrows():
                        try:
                            # Get price and volume
                            close_price = float(row.get('Close', row.get('close', 0)))
                            volume = int(row.get('Volume', row.get('volume', 0)))
                            
                            if close_price <= 0:
                                continue
                            
                            # Get timestamp
                            if hasattr(idx, 'timestamp'):
                                timestamp = idx.timestamp()
                            elif hasattr(idx, 'to_pydatetime'):
                                timestamp = idx.to_pydatetime().timestamp()
                            else:
                                timestamp = datetime.now(KOLKATA_TZ).timestamp()
                            
                            # Create a tick-like data structure and process it
                            tick_data = {
                                'instrument_token': token,
                                'trading_symbol': symbol,
                                'last_price': close_price,
                                'volume': volume,
                                'exchange_timestamp': timestamp,
                                'ohlc': {
                                    'open': float(row.get('Open', row.get('open', close_price))),
                                    'high': float(row.get('High', row.get('high', close_price))),
                                    'low': float(row.get('Low', row.get('low', close_price))),
                                    'close': close_price,
                                }
                            }
                            
                            # Process the tick to build candles
                            candle_store.process_tick(tick_data)
                            total_candles += 1
                            
                        except Exception as row_err:
                            self.logger.debug(f"Error processing row for {symbol}: {row_err}")
                            continue
                    
                    loaded += 1
                    
                except Exception as sym_err:
                    self.logger.debug(f"Error loading symbol {symbol}: {sym_err}")
                    continue
            
            self.logger.info(f"Loaded {loaded} instruments ({total_candles} candles) from {pkl_path}")
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
            
            # Check for ticks.json
            ticks_json = os.path.join(self.data_dir, "ticks.json")
            if os.path.exists(ticks_json):
                files_to_commit.append(ticks_json)
            
            # Also check for date-suffixed pkl files
            today_suffix = datetime.now(KOLKATA_TZ).strftime('%d%m%Y')
            dated_daily = os.path.join(self.data_dir, f"stock_data_{today_suffix}.pkl")
            if os.path.exists(dated_daily) and dated_daily not in files_to_commit:
                files_to_commit.append(dated_daily)
            
            dated_intraday = os.path.join(self.data_dir, f"intraday_stock_data_{today_suffix}.pkl")
            if os.path.exists(dated_intraday) and dated_intraday not in files_to_commit:
                files_to_commit.append(dated_intraday)
            
            if not files_to_commit:
                self.logger.warning("No pkl/json files to commit")
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








