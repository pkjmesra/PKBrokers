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
- Automatic data freshness validation and refresh before sending/committing

"""

import gzip
import json
import logging
import os
import pickle
import shutil
import zipfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests

from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger

# Maximum rows to keep for daily stock data (approximately 1 year of trading data)
MAX_DAILY_ROWS = 251

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

# Staleness threshold (seconds)
MAX_STALE_SECONDS = 120  # 2 minutes
MAX_NETWORK_TIMEOUT = 20 # 20 seconds for network operations
SECONDS_IN_1_MINUTE = 60  # 60 seconds

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
    - Automatic freshness validation with 2-minute staleness threshold
    - Auto-refresh before sending or committing stale data
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
        self.last_periodic_commit = None  # Track last periodic commit time
        self._refresh_count = 0  # Counter for debugging
        
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
        _, file_name = Archiver.afterMarketStockDataExists()
        if file_name is not None and len(file_name) > 0:
            date_str = file_name.replace(".pkl", "").replace("stock_data_", "")
        else:
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
            
            self.logger.debug(f"Loaded {len(holiday_dates)} holidays for {current_year}")
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
        
        time_to_close = (market_close - now).total_seconds() / SECONDS_IN_1_MINUTE
        
        return 0 <= time_to_close <= minutes_before
    
    def refresh_pkl_if_stale(self, candle_store, max_stale_seconds: int = MAX_STALE_SECONDS) -> bool:
        """
        Refresh the daily pkl file if it's stale by more than max_stale_seconds.
        
        This method checks the current pkl freshness and regenerates it using the
        latest candle store data if it's stale. This ensures that any data request
        or commit operation always works with the most recent data.
        
        Args:
            candle_store: InMemoryCandleStore instance
            max_stale_seconds: Maximum allowed staleness in seconds (default: 120)
            
        Returns:
            True if pkl was refreshed, False if already fresh or refresh failed
        """
        pkl_path = self.get_daily_pkl_path()
        self._refresh_count += 1
        
        # Check freshness
        is_fresh, latest_date, missing_days, last_trading_date, latest_time, stale_seconds = self.validate_pkl_freshness(pkl_path)
        
        if is_fresh:
            self.logger.debug(f"[Refresh #{self._refresh_count}] Daily pkl is fresh (stale: {stale_seconds}s < {max_stale_seconds}s)")
            return False
        
        # Log staleness details
        if stale_seconds > 0:
            self.logger.warning(
                f"[Refresh #{self._refresh_count}] Daily pkl is stale by {stale_seconds} seconds "
                f"(>{max_stale_seconds}s). Latest data from {latest_date} {latest_time or ''}. Refreshing..."
            )
        elif missing_days > 0:
            self.logger.warning(
                f"[Refresh #{self._refresh_count}] Daily pkl is missing {missing_days} trading days. "
                f"Latest data from {latest_date}. Refreshing..."
            )
        
        # Refresh the pkl with latest candle data
        start_time = datetime.now()
        success, _ = self.export_daily_candles_to_pkl(candle_store, merge_with_historical=True)
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if success:
            self.logger.info(f"[Refresh #{self._refresh_count}] ✅ Daily pkl refreshed successfully in {elapsed:.2f}s")
            
            # Verify freshness after refresh
            is_fresh, latest_date, missing_days, last_trading_date, latest_time, stale_seconds = self.validate_pkl_freshness(pkl_path)
            if is_fresh:
                self.logger.info(f"[Refresh #{self._refresh_count}] ✓ Post-refresh validation: Fresh - {latest_date} {latest_time}")
            else:
                self.logger.warning(f"[Refresh #{self._refresh_count}] ⚠ Post-refresh validation: Still stale by {stale_seconds}s")
            
            return True
        else:
            self.logger.error(f"[Refresh #{self._refresh_count}] ❌ Failed to refresh daily pkl")
            return False
    
    def refresh_intraday_pkl_if_stale(self, candle_store, max_stale_seconds: int = MAX_STALE_SECONDS) -> bool:
        """
        Refresh the intraday pkl file if it's stale by more than max_stale_seconds.
        
        Args:
            candle_store: InMemoryCandleStore instance
            max_stale_seconds: Maximum allowed staleness in seconds (default: 120)
            
        Returns:
            True if pkl was refreshed, False if already fresh or refresh failed
        """
        pkl_path = self.get_intraday_pkl_path()
        
        # Check if file exists and get its last modified time as proxy for freshness
        # For intraday, we want it updated frequently during market hours
        if not os.path.exists(pkl_path):
            self.logger.info("Intraday pkl does not exist, creating...")
            success, _, _ = self.export_intraday_candles_to_pkl(candle_store)
            return success
        
        # Check last modified time
        mod_time = datetime.fromtimestamp(os.path.getmtime(pkl_path), tz=KOLKATA_TZ)
        now = datetime.now(KOLKATA_TZ)
        seconds_old = (now - mod_time).total_seconds()
        
        # During market hours, refresh if older than max_stale_seconds
        is_market_open = self.is_market_open()
        
        if is_market_open and seconds_old > max_stale_seconds:
            self.logger.info(f"Intraday pkl is {seconds_old:.0f}s old (> {max_stale_seconds}s). Refreshing...")
            success, _, _ = self.export_intraday_candles_to_pkl(candle_store)
            return success
        
        return False
    
    def ensure_data_freshness(self, candle_store, max_stale_seconds: int = MAX_STALE_SECONDS) -> Tuple[bool, bool]:
        """
        Ensure both daily and intraday pkl files are fresh.
        
        This is a convenience method to refresh both data files if needed.
        
        Args:
            candle_store: InMemoryCandleStore instance
            max_stale_seconds: Maximum allowed staleness in seconds
            
        Returns:
            Tuple of (daily_refreshed, intraday_refreshed)
        """
        daily_refreshed = self.refresh_pkl_if_stale(candle_store, max_stale_seconds)
        intraday_refreshed = self.refresh_intraday_pkl_if_stale(candle_store, max_stale_seconds)
        
        if daily_refreshed or intraday_refreshed:
            self.logger.info(f"Data freshness ensured - Daily: {daily_refreshed}, Intraday: {intraday_refreshed}")
        
        return daily_refreshed, intraday_refreshed
    
    def download_from_github(self, file_type: str = "daily", validate_freshness: bool = True, 
                            candle_store=None, max_stale_seconds: int = MAX_STALE_SECONDS) -> Tuple[bool, Optional[str], int]:
        """
        Download pkl file from GitHub actions-data-download branch.
        
        Searches for pkl files in multiple locations and date formats:
        - actions-data-download/stock_data_DDMMYYYY.pkl
        - results/Data/stock_data_DDMMYYYY.pkl
        - Also tries recent dates going back up to 10 days
        
        If the downloaded data is stale and candle_store is provided, it will be
        refreshed automatically.
        
        Args:
            file_type: "daily" or "intraday"
            validate_freshness: If True, check if data is fresh
            candle_store: If provided, refresh stale data
            max_stale_seconds: Maximum allowed staleness in seconds
            
        Returns:
            Tuple of (success, file_path, stale_seconds)
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
                    response = requests.get(url, timeout=MAX_NETWORK_TIMEOUT)
                    
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
                                
                                # Validate freshness and refresh if needed
                                stale_seconds = 0
                                if validate_freshness and candle_store and file_type == "daily":
                                    is_fresh, _, missing_days, _, _, stale_seconds = self.validate_pkl_freshness(output_path)
                                    
                                    if not is_fresh and (missing_days > 0 or stale_seconds > max_stale_seconds):
                                        self.logger.warning(
                                            f"Downloaded pkl is stale by {missing_days} trading days or "
                                            f"{stale_seconds} seconds. Refreshing with current candle store..."
                                        )
                                        
                                        # Refresh with current candle store data
                                        refresh_success, _ = self.export_daily_candles_to_pkl(
                                            candle_store, merge_with_historical=True
                                        )
                                        
                                        if refresh_success:
                                            self.logger.info("✅ Stale data refreshed successfully")
                                            # Re-check freshness
                                            is_fresh, _, missing_days, _, _, stale_seconds = self.validate_pkl_freshness(output_path)
                                            if is_fresh:
                                                self.logger.info("✅ Data is now fresh after refresh")
                                        
                                        # If still stale, trigger history download
                                        if not is_fresh and missing_days > 0:
                                            self.trigger_history_download_workflow(missing_days)
                                
                                return True, output_path, stale_seconds
                        except (pickle.UnpicklingError, EOFError) as e:
                            self.logger.debug(f"Invalid pkl file from {url}: {e}")
                            continue
                        
                except requests.RequestException as e:
                    self.logger.debug(f"Failed to download from {url}: {e}")
                    continue
            
            self.logger.warning(f"Could not download {file_type} pkl from GitHub after trying {len(urls_to_try)} URLs")
            return False, None, 0
            
        except Exception as e:
            self.logger.error(f"Error downloading from GitHub: {e}")
            return False, None, 0
    
    def validate_pkl_freshness(self, pkl_path: str) -> Tuple[bool, Optional[datetime], int, Optional[datetime], Optional[datetime.time], int]:
        """
        Validate if pkl file data is fresh (has data up to current market time).
        
        This method checks the latest timestamp in the pkl file and compares it
        with the expected reference time (current time during market hours, or
        last market close outside market hours). Data is considered fresh if it's
        within the acceptable staleness threshold (handled by caller).
        
        Args:
            pkl_path: Path to pkl file
            
        Returns:
            Tuple of (is_fresh, latest_data_date, missing_trading_days, last_trading_date, latest_time, stale_seconds)
            - is_fresh: True if data includes recent ticks (within caller's threshold)
            - latest_data_date: Date of most recent data point
            - missing_trading_days: Number of trading days missing (0 if fresh)
            - last_trading_date: Last trading date for comparison
            - latest_time: Time of most recent data point
            - stale_seconds: Number of seconds the data is stale (0 if fresh)
        """
        try:
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            from datetime import datetime, time, timedelta
            import pytz
            
            if not os.path.exists(pkl_path):
                return False, None, 0, None, None, 0
            
            with open(pkl_path, 'rb') as f:
                data = pickle.load(f)
            
            if not data:
                return False, None, 0, None, None, 0
            
            # Find the latest date and time across all stocks
            latest_date = None
            latest_datetime = None  # Full datetime with timezone
            latest_time = None
            
            for symbol, df in data.items():
                if hasattr(df, 'index') and len(df.index) > 0:
                    stock_last_idx = df.index[-1]
                    stock_datetime = None
                    
                    # Extract full datetime based on index type
                    if hasattr(stock_last_idx, 'to_pydatetime'):
                        # Pandas Timestamp
                        stock_datetime = stock_last_idx.to_pydatetime()
                    elif hasattr(stock_last_idx, 'tz'):
                        # Timezone-aware datetime
                        stock_datetime = stock_last_idx
                    elif isinstance(stock_last_idx, (datetime, str)):
                        # Convert string or datetime
                        if isinstance(stock_last_idx, str):
                            try:
                                stock_datetime = datetime.fromisoformat(stock_last_idx)
                            except:
                                continue
                        else:
                            stock_datetime = stock_last_idx
                    
                    if stock_datetime is None:
                        continue
                    
                    # Ensure timezone-aware (assume IST if naive)
                    if hasattr(stock_datetime, 'tzinfo') and stock_datetime.tzinfo is None:
                        stock_datetime = pytz.timezone('Asia/Kolkata').localize(stock_datetime)
                    
                    # Get date and time
                    stock_date = stock_datetime.date()
                    stock_time = stock_datetime.time()
                    
                    if latest_datetime is None or stock_datetime > latest_datetime:
                        latest_datetime = stock_datetime
                        latest_date = stock_date
                        latest_time = stock_time
                    elif stock_date == latest_date and stock_time and latest_time and stock_time > latest_time:
                        latest_datetime = stock_datetime
                        latest_time = stock_time
            
            if latest_datetime is None:
                return False, None, 0, None, None, 0
            
            # Get current time in IST
            KOLKATA_TZ = pytz.timezone('Asia/Kolkata')
            now = datetime.now(KOLKATA_TZ)
            
            # Define market hours
            MARKET_OPEN_TIME = time(9, 15, 0)
            MARKET_CLOSE_TIME = time(15, 30, 0)  # Note: using 15:30, not 15:29
            current_time = now.time()
            current_date = now.date()
            
            # Check if today is a trading day
            is_trading_day = self.is_trading_day(now)
            
            # Determine the reference time for freshness comparison
            if is_trading_day and current_time >= MARKET_OPEN_TIME:
                # During market hours - compare against current time
                reference_datetime = now
                self.logger.debug(f"Market hours - comparing against current time: {reference_datetime}")
            else:
                # Outside market hours or holiday - compare against last market close
                last_trading_date = PKDateUtilities.tradingDate()
                if hasattr(last_trading_date, 'date'):
                    last_trading_date = last_trading_date.date()
                
                reference_datetime = KOLKATA_TZ.localize(datetime.combine(
                    last_trading_date, 
                    MARKET_CLOSE_TIME
                ))
                self.logger.debug(f"Non-market hours - comparing against last close: {reference_datetime}")
            
            # Calculate stale seconds
            if latest_datetime >= reference_datetime:
                # Data is fresh (includes current time or last close)
                is_fresh = True
                stale_seconds = 0
                self.logger.debug(f"Pkl data is fresh. Latest: {latest_datetime}, Reference: {reference_datetime}")
            else:
                # Data is stale
                is_fresh = False
                stale_seconds = int((reference_datetime - latest_datetime).total_seconds())
                
                # Calculate missing trading days for historical context
                last_trading_date = PKDateUtilities.tradingDate()
                if hasattr(last_trading_date, 'date'):
                    last_trading_date = last_trading_date.date()
                
                if latest_date < last_trading_date:
                    missing_days = PKDateUtilities.trading_days_between(latest_date, last_trading_date)
                else:
                    missing_days = 0
                
                # Detailed logging based on scenario
                if is_trading_day and current_time >= MARKET_OPEN_TIME:
                    # During market hours
                    self.logger.warning(f"Pkl data is stale during market hours. "
                                    f"Latest: {latest_datetime}, Current: {now}, "
                                    f"Stale by {stale_seconds} seconds ({stale_seconds/SECONDS_IN_1_MINUTE:.1f} minutes)")
                elif latest_date == current_date and not is_trading_day:
                    # Today is holiday, data from today (shouldn't happen)
                    self.logger.debug(f"Pkl data is from today ({current_date}) which is a holiday. "
                                    f"Last trading day was {last_trading_date}")
                else:
                    # Outside market hours
                    self.logger.warning(f"Pkl data is stale. Latest: {latest_datetime}, "
                                    f"Reference: {reference_datetime}, "
                                    f"Missing {missing_days} trading days, Stale by {stale_seconds} seconds")
            
            # Get last trading date for return value
            last_trading_date = PKDateUtilities.tradingDate()
            if hasattr(last_trading_date, 'date'):
                last_trading_date = last_trading_date.date()
            
            return is_fresh, latest_date, missing_days if not is_fresh else 0, last_trading_date, latest_time, stale_seconds
            
        except Exception as e:
            self.logger.error(f"Error validating pkl freshness: {e}")
            return False, None, 0, None, None, 0
    
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
    
    def ensure_data_freshness_and_commit(self, pkl_path: str = None, candle_store=None) -> bool:
        """
        Ensure pkl data is fresh. If stale, refresh it and commit updated data.
        
        This is the main entry point for the freshness check workflow.
        
        Args:
            pkl_path: Path to pkl file (defaults to daily pkl)
            candle_store: InMemoryCandleStore instance (required for refresh)
            
        Returns:
            True if data is fresh or was successfully updated
        """
        try:
            if pkl_path is None:
                pkl_path = self.get_daily_pkl_path()
            
            is_fresh, data_date, missing_days, trading_date, latest_time, stale_seconds = self.validate_pkl_freshness(pkl_path)
            
            if is_fresh:
                self.logger.info("Data is already fresh")
                return True
            
            if missing_days > 0 or stale_seconds > 0:
                self.logger.warning(f"Data is stale by {missing_days} trading days or {stale_seconds} seconds. ")
                
                # If we have candle_store, try to refresh immediately
                if candle_store:
                    self.logger.info("Attempting to refresh data with current candle store...")
                    success, _ = self.export_daily_candles_to_pkl(candle_store, merge_with_historical=True)
                    
                    if success:
                        self.logger.info("Data refreshed successfully")
                        return True
                
                # If refresh failed or no candle_store, trigger history download
                self.logger.info("Triggering history download workflow...")
                triggered = self.trigger_history_download_workflow(missing_days)
                
                if triggered:
                    self.logger.info("History download workflow triggered. Fresh data will be available after workflow completes.")
                    return True
                else:
                    self.logger.warning("Failed to trigger history download workflow")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error ensuring data freshness: {e}")
            return False
    
    def prepare_daily_pkl_for_sending(self, candle_store, max_stale_seconds: int = MAX_STALE_SECONDS) -> bool:
        """
        Ensures the daily_candles.pkl file is fresh, updating it if necessary.
        
        This method should be called before sending the pkl file to ensure the
        recipient gets the most recent data available.
        
        Args:
            candle_store: InMemoryCandleStore instance
            max_stale_seconds: Maximum allowed staleness before refresh
            
        Returns:
            True if the PKL is fresh or was successfully updated, False otherwise.
        """
        pkl_path = self.get_daily_pkl_path()
        
        # Check freshness of the existing daily_candles.pkl
        is_fresh, data_date, missing_days, trading_date, latest_time, stale_seconds = self.validate_pkl_freshness(pkl_path)
        
        # If fresh and within threshold, use as-is
        if is_fresh and stale_seconds <= max_stale_seconds and os.path.exists(pkl_path):
            self.logger.info(f"daily_candles.pkl is fresh (stale: {stale_seconds}s <= {max_stale_seconds}s). Ready to send.")
            return True
        
        # Otherwise, regenerate
        if stale_seconds > max_stale_seconds:
            self.logger.info(f"daily_candles.pkl is stale by {stale_seconds}s (>{max_stale_seconds}s). Regenerating...")
        elif missing_days > 0:
            self.logger.warning(f"daily_candles.pkl is missing {missing_days} trading days. Regenerating...")
        else:
            self.logger.warning("daily_candles.pkl does not exist or is invalid. Regenerating...")
        
        # Regenerate/update the daily PKL with current data from candle_store
        success, _ = self.export_daily_candles_to_pkl(candle_store, merge_with_historical=True)
        
        if success:
            self.logger.info("✅ Successfully regenerated daily_candles.pkl.")
            
            # Verify after regeneration
            is_fresh, data_date, missing_days, trading_date, latest_time, stale_seconds = self.validate_pkl_freshness(pkl_path)
            self.logger.info(f"Post-regeneration: Fresh={is_fresh}, Date={data_date} {latest_time}, Stale={stale_seconds}s")
            
            return True
        else:
            self.logger.error("❌ Failed to regenerate daily_candles.pkl.")
            return False

    def export_daily_candles_to_pkl(self, candle_store, merge_with_historical: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Export daily candles from InMemoryCandleStore to pkl file.
        Merges with historical data from GitHub to create complete ~35MB+ pkl files.
        All timestamps are stored as timezone-aware IST (Asia/Kolkata).
        
        Args:
            candle_store: InMemoryCandleStore instance
            merge_with_historical: Whether to merge with historical pkl from GitHub
            
        Returns:
            Tuple of (success, file_path)
        """
        try:
            import pandas as pd
            from pandas.api.types import is_datetime64_any_dtype
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            
            output_path = self.get_daily_pkl_path()
            data = {}
            today_trading_date = PKDateUtilities.tradingDate()
            
            # Helper function to normalize timestamps to IST (timezone-aware)
            def normalize_to_ist(df):
                """Convert DataFrame index to timezone-aware IST."""
                if df is None or not hasattr(df, 'index'):
                    return df
                
                try:
                    if hasattr(df.index, 'tz'):
                        # If it has timezone, convert to IST
                        if df.index.tz is not None:
                            df = df.copy()
                            df.index = df.index.tz_convert(KOLKATA_TZ)
                        else:
                            # If naive, assume UTC and convert to IST
                            df = df.copy()
                            df.index = pd.DatetimeIndex(df.index).tz_localize('UTC').tz_convert(KOLKATA_TZ)
                    else:
                        # Not a DatetimeIndex, try to convert
                        df = df.copy()
                        df.index = pd.DatetimeIndex(df.index).tz_localize('UTC').tz_convert(KOLKATA_TZ)
                except Exception as e:
                    self.logger.debug(f"Error normalizing timezone: {e}")
                    # Fallback: try to set as IST directly
                    try:
                        df = df.copy()
                        df.index = pd.DatetimeIndex(df.index).tz_localize(KOLKATA_TZ)
                    except:
                        pass
                return df
            
            # First, try to load existing historical data from GitHub
            if merge_with_historical:
                try:
                    self.logger.info("Attempting to download historical pkl from GitHub for merge...")
                    success, historical_path, _ = self.download_from_github(file_type="daily", validate_freshness=False)
                    if success and historical_path and os.path.exists(historical_path):
                        with open(historical_path, 'rb') as f:
                            historical_data = pickle.load(f)
                        
                        self.logger.info(f"Downloaded historical pkl with {len(historical_data)} instruments")
                        
                        # Convert historical data to proper format and normalize to IST
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
                                    # Normalize to IST
                                    df = normalize_to_ist(df)
                                    data[symbol] = df
                            elif hasattr(df_or_dict, 'index'):
                                # Normalize to IST for DataFrame
                                data[symbol] = normalize_to_ist(df_or_dict)
                        
                        self.logger.info(f"Loaded {len(data)} instruments from historical pkl into merge buffer")
                    else:
                        self.logger.warning("Could not download historical pkl from GitHub - will export only today's candle data")
                except Exception as he:
                    self.logger.warning(f"Could not load historical data: {he}")
            
            # Now add today's candles from the candle store
            today_count = 0
            latest_timestamp_any = None  # Track the absolute latest timestamp across all instruments

            with candle_store.lock:
                for token, instrument in candle_store.instruments.items():
                    symbol = candle_store.instrument_symbols.get(token, str(token))
                    
                    # Get all daily candles including current
                    day_candles = list(instrument.candles.get('day', []))
                    current_day = instrument.current_candle.get('day')
                    
                    # Determine the most recent daily candle data
                    latest_candle = None
                    latest_candle_time = None
                    
                    if current_day and current_day.tick_count > 0:
                        latest_candle = current_day
                        latest_candle_time = datetime.fromtimestamp(current_day.timestamp, tz=KOLKATA_TZ)
                    
                    # Also check completed candles for any more recent ones
                    for candle in day_candles:
                        candle_time = datetime.fromtimestamp(candle.timestamp, tz=KOLKATA_TZ)
                        if latest_candle_time is None or candle_time > latest_candle_time:
                            latest_candle = candle
                            latest_candle_time = candle_time
                    
                    if latest_candle is None:
                        continue
                    
                    # Use last_tick_time for current candles, timestamp for completed
                    if latest_candle.is_complete or latest_candle.last_tick_time == 0:
                        dt_ist = datetime.fromtimestamp(latest_candle.timestamp, tz=KOLKATA_TZ)
                    else:
                        dt_ist = datetime.fromtimestamp(latest_candle.last_tick_time, tz=KOLKATA_TZ)
                    
                    # Track the absolute latest timestamp across all instruments
                    if latest_timestamp_any is None or dt_ist > latest_timestamp_any:
                        latest_timestamp_any = dt_ist
                    
                    # Only include today's data from InMemoryCandleStore
                    if dt_ist.date() == today_trading_date:
                        # For the daily export, we want to use the most recent candle data
                        # This means using the current candle's close price as the latest price
                        rows = [{
                            'Date': dt_ist,
                            'Open': latest_candle.open,
                            'High': latest_candle.high,
                            'Low': latest_candle.low if latest_candle.low != float('inf') else latest_candle.open,
                            'Close': latest_candle.close,
                            'Volume': latest_candle.volume,
                        }]
                        
                        new_df = pd.DataFrame(rows)
                        new_df.set_index('Date', inplace=True)
                        
                        # Ensure index is timezone-aware IST
                        if not hasattr(new_df.index, 'tz') or new_df.index.tz is None:
                            new_df.index = new_df.index.tz_localize(KOLKATA_TZ)
                        elif new_df.index.tz != KOLKATA_TZ:
                            new_df.index = new_df.index.tz_convert(KOLKATA_TZ)
                        
                        # Merge with historical data if exists
                        if symbol in data:
                            existing_df = data[symbol]
                            # Ensure existing data is also IST-aware
                            existing_df = normalize_to_ist(existing_df)
                            
                            # Remove today's data from historical_data if present (we'll replace it with fresh candle_store data)
                            existing_df = existing_df[existing_df.index.date != today_trading_date]
                            
                            # Combine and remove duplicates (keep latest)
                            combined = pd.concat([existing_df, new_df])
                            combined = combined[~combined.index.duplicated(keep='last')]
                            # Sort index - all timestamps are IST-aware now
                            combined.sort_index(inplace=True)
                            data[symbol] = combined
                        else:
                            data[symbol] = new_df
                        today_count += 1

            # After processing all symbols, update the pkl modification time to match the latest tick
            if latest_timestamp_any and os.path.exists(output_path):
                # Update the file's modification time to match the latest tick time
                # This helps with freshness checks that look at file mtime
                mod_time = latest_timestamp_any.timestamp()
                os.utime(output_path, (mod_time, mod_time))
                self.logger.debug(f"Updated pkl mtime to {latest_timestamp_any}")
            
            if data:
                # Trim each stock to most recent 251 rows before saving
                trimmed_count = 0
                for symbol in list(data.keys()):
                    try:
                        df = data[symbol]
                        if hasattr(df, '__len__') and len(df) > MAX_DAILY_ROWS:
                            # Ensure index is sorted before taking tail
                            df = df.sort_index()
                            data[symbol] = df.tail(MAX_DAILY_ROWS)
                            trimmed_count += 1
                    except Exception as e:
                        self.logger.debug(f"Error trimming {symbol}: {e}")
                        continue
                
                if trimmed_count > 0:
                    self.logger.info(f"Trimmed {trimmed_count} stocks to {MAX_DAILY_ROWS} rows each")
                
                with open(output_path, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                
                file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
                self.logger.info(f"Exported {len(data)} instruments ({today_count} with today's data) to {output_path} ({file_size:.2f} MB)")
                
                # Verify timezone of first few symbols (debug)
                for symbol in list(data.keys())[:3]:
                    df = data[symbol]
                    if hasattr(df.index, 'tz'):
                        self.logger.debug(f"{symbol} timezone: {df.index.tz}")
                
                return True, output_path
            else:
                self.logger.warning("No daily candle data to export")
                return False, None
                
        except Exception as e:
            self.logger.error(f"Error exporting daily candles: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False, None
    
    def export_intraday_candles_to_pkl(self, candle_store) -> Tuple[bool, Optional[str], Optional[datetime]]:
        """
        Export 1-minute candles from InMemoryCandleStore to pkl file.
        
        Args:
            candle_store: InMemoryCandleStore instance
            
        Returns:
            Tuple of (success, file_path, latest_date)
        """
        try:
            import pandas as pd
            
            latest_date = None
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
                        if latest_date is None or dt > latest_date:
                            latest_date = dt
                    
                    if rows:
                        df = pd.DataFrame(rows)
                        df.set_index('Date', inplace=True)
                        data[symbol] = df
            
            if data:
                with open(output_path, 'wb') as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                
                file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
                self.logger.info(f"Exported {len(data)} intraday instruments to {output_path} ({file_size:.2f} MB), latest: {latest_date}")
                return True, output_path, latest_date
            else:
                self.logger.warning("No intraday candle data to export")
                return False, None, latest_date
                
        except Exception as e:
            self.logger.error(f"Error exporting intraday candles: {e}")
            return False, None, None
    
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
                _, today_suffix = Archiver.afterMarketStockDataExists()
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
    
    def commit_pkl_files(self, branch_name: str = "actions-data-download", candle_store=None, max_stale_seconds: int = MAX_STALE_SECONDS) -> bool:
        """
        Commit pkl files to PKScreener's GitHub repository (actions-data-download branch).
        
        Ensures data is fresh before committing by refreshing if needed.
        
        Uses GitHub API to commit across repositories.
        
        Args:
            branch_name: Branch to commit to
            candle_store: If provided, refresh stale data before commit
            max_stale_seconds: Maximum allowed staleness before refresh
            
        Returns:
            True if commit was successful
        """
        try:
            from PKDevTools.classes.PKDateUtilities import PKDateUtilities
            import base64
            import requests
            
            # Get GitHub token
            github_token = os.environ.get('CI_PAT') or os.environ.get('GITHUB_TOKEN')
            if not github_token:
                self.logger.warning("No GitHub token found, cannot commit pkl files")
                return False
            
            # Refresh data if needed before committing
            if candle_store:
                self.logger.info("Refreshing data before commit...")
                daily_refreshed, intraday_refreshed = self.ensure_data_freshness(candle_store, max_stale_seconds)
                if daily_refreshed or intraday_refreshed:
                    self.logger.info(f"Data refreshed before commit - Daily: {daily_refreshed}, Intraday: {intraday_refreshed}")
            
            files_to_commit = []
            from PKDevTools.classes import Archiver
            _ , cache_file_name = Archiver.afterMarketStockDataExists()
            today_suffix = cache_file_name.replace(".pkl", "").replace("stock_data_", "")
            
            # Define minimum file sizes (in bytes)
            DAILY_PKL_MIN_SIZE = 25 * 1024 * 1024  # 25 MB
            INTRADAY_PKL_MIN_SIZE = 1 * 1024 * 1024  # 1 MB
            
            # Check for daily pkl with size validation
            daily_pkl = self.get_daily_pkl_path()
            if os.path.exists(daily_pkl):
                file_size = os.path.getsize(daily_pkl)
                if file_size >= DAILY_PKL_MIN_SIZE:
                    files_to_commit.append((daily_pkl, f"actions-data-download/stock_data_{today_suffix}.pkl"))
                    files_to_commit.append((daily_pkl, "actions-data-download/daily_candles.pkl"))
                    self.logger.debug(f"Daily pkl size: {file_size/(1024*1024):.2f} MB - ✓ Valid")
                else:
                    self.logger.warning(f"Daily pkl size ({file_size/(1024*1024):.2f} MB) below minimum ({DAILY_PKL_MIN_SIZE/(1024*1024):.0f} MB) - skipping")
            
            # Check for intraday pkl with size validation
            intraday_pkl = self.get_intraday_pkl_path()
            if os.path.exists(intraday_pkl):
                file_size = os.path.getsize(intraday_pkl)
                if file_size >= INTRADAY_PKL_MIN_SIZE:
                    files_to_commit.append((intraday_pkl, f"actions-data-download/intraday_stock_data_{today_suffix}.pkl"))
                    files_to_commit.append((intraday_pkl, "actions-data-download/intraday_1m_candles.pkl"))
                    self.logger.debug(f"Intraday pkl size: {file_size/(1024*1024):.2f} MB - ✓ Valid")
                else:
                    self.logger.warning(f"Intraday pkl size ({file_size/(1024*1024):.2f} MB) below minimum ({INTRADAY_PKL_MIN_SIZE/(1024*1024):.0f} MB) - skipping")
            
            # Check for date-suffixed daily pkl with size validation
            dated_daily = os.path.join(self.data_dir, f"stock_data_{today_suffix}.pkl")
            if os.path.exists(dated_daily) and dated_daily != daily_pkl:
                file_size = os.path.getsize(dated_daily)
                if file_size >= DAILY_PKL_MIN_SIZE:
                    files_to_commit.append((dated_daily, f"actions-data-download/stock_data_{today_suffix}.pkl"))
                    self.logger.debug(f"Dated daily pkl size: {file_size/(1024*1024):.2f} MB - ✓ Valid")
                else:
                    self.logger.warning(f"Dated daily pkl size ({file_size/(1024*1024):.2f} MB) below minimum ({DAILY_PKL_MIN_SIZE/(1024*1024):.0f} MB) - skipping")
            
            # Check for date-suffixed intraday pkl with size validation
            dated_intraday = os.path.join(self.data_dir, f"intraday_stock_data_{today_suffix}.pkl")
            if os.path.exists(dated_intraday) and dated_intraday != intraday_pkl:
                file_size = os.path.getsize(dated_intraday)
                if file_size >= INTRADAY_PKL_MIN_SIZE:
                    files_to_commit.append((dated_intraday, f"actions-data-download/intraday_stock_data_{today_suffix}.pkl"))
                    self.logger.debug(f"Dated intraday pkl size: {file_size/(1024*1024):.2f} MB - ✓ Valid")
                else:
                    self.logger.warning(f"Dated intraday pkl size ({file_size/(1024*1024):.2f} MB) below minimum ({INTRADAY_PKL_MIN_SIZE/(1024*1024):.0f} MB) - skipping")
            
            if not files_to_commit:
                self.logger.warning("No valid pkl files to commit (all files below size thresholds or missing)")
                return False
            
            # Remove duplicate remote paths (keep first occurrence)
            unique_files = {}
            for local_path, remote_path in files_to_commit:
                if remote_path not in unique_files:
                    unique_files[remote_path] = local_path
            
            files_to_commit = [(local_path, remote_path) for remote_path, local_path in unique_files.items()]
            
            self.logger.info(f"Preparing to commit {len(files_to_commit)} unique files after size validation")
            
            # Use GitHub API to commit to PKScreener repo
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            
            repo = "pkjmesra/PKScreener"
            api_base = f"https://api.github.com/repos/{repo}"
            
            committed_files = []
            for local_path, remote_path in files_to_commit:
                try:
                    # Double-check file size before committing
                    file_size = os.path.getsize(local_path)
                    is_daily = 'daily' in remote_path or 'stock_data' in remote_path
                    is_intraday = 'intraday' in remote_path
                    
                    min_size = DAILY_PKL_MIN_SIZE if is_daily else INTRADAY_PKL_MIN_SIZE
                    
                    if file_size < min_size:
                        self.logger.warning(f"File {remote_path} size ({file_size/(1024*1024):.2f} MB) dropped below minimum during processing - skipping")
                        continue
                    
                    # Read file content
                    with open(local_path, 'rb') as f:
                        content = base64.b64encode(f.read()).decode('utf-8')
                    
                    # Get current file SHA (if exists)
                    sha = None
                    get_url = f"{api_base}/contents/{remote_path}?ref={branch_name}"
                    resp = requests.get(get_url, headers=headers)
                    if resp.status_code == 200:
                        sha = resp.json().get('sha')
                    elif resp.status_code != 404:
                        self.logger.warning(f"Unexpected response checking {remote_path}: {resp.status_code}")
                    
                    # Create/update file
                    put_url = f"{api_base}/contents/{remote_path}"
                    commit_msg = f"[DataSharingManager] Update {os.path.basename(remote_path)} - {PKDateUtilities.currentDateTime()}"
                    
                    data = {
                        "message": commit_msg,
                        "content": content,
                        "branch": branch_name
                    }
                    if sha:
                        data["sha"] = sha
                    
                    resp = requests.put(put_url, headers=headers, json=data)
                    
                    if resp.status_code in [200, 201]:
                        self.logger.info(f"✅ Committed {remote_path} ({file_size/(1024*1024):.2f} MB) to PKScreener/{branch_name}")
                        committed_files.append(remote_path)
                    else:
                        self.logger.warning(f"Failed to commit {remote_path}: {resp.status_code} {resp.text[:200]}")
                        
                except Exception as e:
                    self.logger.error(f"Error committing {local_path}: {e}")
            
            if committed_files:
                self.last_commit_time = datetime.now(KOLKATA_TZ)
                self.logger.info(f"✅ Successfully committed {len(committed_files)} files to PKScreener")
                return True
            
            self.logger.warning("No files were successfully committed")
            return False
            
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
