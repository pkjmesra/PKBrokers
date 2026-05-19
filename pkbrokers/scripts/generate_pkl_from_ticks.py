#!/usr/bin/env python3
"""
===============================================================================
PKL Generator for PKBrokers - Unified Historical & Intraday Data Processor
===============================================================================

DESCRIPTION
-----------
This script generates processed stock data files (PKL format) from multiple
sources including GitHub historical data, ticks.json (real-time/streaming),
and SQLite databases. It serves as the core data pipeline for the PKBrokers
trading system, producing both daily aggregated candles and intraday 1-minute
candles.

ARCHITECTURE OVERVIEW
---------------------
┌─────────────────────────────────────────────────────────────────────────┐
│                         INPUT SOURCES                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  • GitHub Historical PKL (last 30 days, ~37MB)                          │
│  • Local/Remote ticks.json (real-time tick data)                        │
│  • SQLite Database (instrument_history.db)                              │
│  • InMemoryCandleStore (real-time aggregated candles)                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         PROCESSING PIPELINE                              │
├─────────────────────────────────────────────────────────────────────────┤
│  1. Download/Load historical base data from GitHub                       │
│  2. Retrieve current day's aggregated data from InMemoryCandleStore     │
│  3. Load/Download fresh ticks.json data                                 │
│  4. Convert ticks → 1-minute OHLCV candles                              │
│  5. Merge with historical data (deduplication by timestamp)             │
│  6. Trim daily data to 251 rows per stock (1 trading year)              │
│  7. Save as daily_candles.pkl & stock_data_<DDMMYYYY>.pkl               │
│  8. Save intraday_1m_candles.pkl & intraday_stock_data_<DDMMYYYY>.pkl   │
└─────────────────────────────────────────────────────────────────────────┘

KEY FEATURES
------------
• **Intelligent Freshness Detection**: Determines if data needs updates based on
  trading dates, market hours, and actual data timestamps
• **Token-to-Symbol Mapping**: Converts numeric instrument tokens to readable
  trading symbols using KiteInstruments
• **Atomic Writes**: Uses temporary files + rename for safe pickle saves
• **Data Quality Validation**: Validates row counts (≥248 rows per stock) before
  accepting historical data
• **Graceful Fallbacks**: Falls back to alternative sources if primary fails
• **Timezone Normalization**: Converts all timestamps to Asia/Kolkata (IST)

USAGE
-----
    # From ticks.json (default - for market hours with tick merging)
    python generate_pkl_from_ticks.py [--data-dir PATH] [--verbose]
    
    # From SQLite database (for after-market hours, no tick merging)
    python generate_pkl_from_ticks.py --from-db [--db-path PATH] [--data-dir PATH] [--verbose]
    
    # Trigger history download workflow if data is stale
    python generate_pkl_from_ticks.py --trigger-history --past-offset 30

ARGUMENTS
---------
    --data-dir PATH         Output directory for pkl files (default: results/Data)
    --from-db               Load data from SQLite database instead of ticks.json
    --db-path PATH          Path to SQLite database (auto-detected if not specified)
    --trigger-history       Trigger GitHub Actions workflow to download historical data
    --past-offset DAYS      Days to look back for existing pkl files (default: 30)
    --verbose, -v           Verbose output (default: True)

DATA FORMATS
------------
Daily PKL Format (stock_data_<DDMMYYYY>.pkl):
    {
        "RELIANCE": pd.DataFrame with columns ['Open', 'High', 'Low', 'Close', 'Volume'],
        "TCS": pd.DataFrame,
        ...
    }

Intraday PKL Format (intraday_stock_data_<DDMMYYYY>.pkl):
    {
        "RELIANCE": pd.DataFrame with 1-minute OHLCV candles,
        "TCS": pd.DataFrame,
        ...
    }

Ticks.json Format:
    {
        "instrument_token": {
            "trading_symbol": "RELIANCE",
            "last_updated": "2026-05-09T15:29:00",
            "ohlcv": {
                "open": 2450.5,
                "high": 2460.0,
                "low": 2445.0,
                "close": 2455.5,
                "volume": 1234567,
                "timestamp": "2026-05-09T15:29:00"
            }
        }
    }

DEPENDENCIES
------------
    • pandas
    • numpy
    • requests
    • pytz
    • sqlite3 (built-in)
    • PKDevTools (custom)
    • pkbrokers (custom)

===============================================================================
"""

import argparse
import io
import json
import os
import pickle
import sqlite3
import sys
import zipfile
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd
import pytz
import requests

from PKDevTools.classes.PKDateUtilities import PKDateUtilities

from pkbrokers.kite.examples.pkkite import kite_ticks


def log(msg: str, verbose: bool = True):
    """Print timestamped log message if verbose mode is enabled."""
    if verbose:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


KOLKATA_TZ = pytz.timezone("Asia/Kolkata")


# Global cache for token-to-symbol mapping
_token_to_symbol_cache: Dict[int, str] = {}


def safe_pickle_save(data: Dict, filepath: str, verbose: bool = True) -> bool:
    """
    Safely save pickle with validation using atomic write pattern.
    
    This function writes to a temporary file first, verifies the data can be
    read back, then atomically renames to the target path. This prevents
    corruption from partial writes or interruptions.
    
    Args:
        data: Dictionary to pickle
        filepath: Target file path
        verbose: Whether to log progress
        
    Returns:
        True if save successful, False otherwise
    """
    import tempfile
    import shutil
    
    temp_fd, temp_path = tempfile.mkstemp(
        suffix='.pkl.tmp',
        dir=os.path.dirname(filepath) or '.'
    )
    
    try:
        with os.fdopen(temp_fd, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            f.flush()
            os.fsync(f.fileno())
        
        # Verify the file is valid
        with open(temp_path, 'rb') as f:
            test_data = pickle.load(f)
        
        if len(test_data) != len(data):
            log(f"⚠️ Data mismatch during save!", verbose)
            return False
        
        shutil.move(temp_path, filepath)
        log(f"✅ Saved pickle: {filepath}", verbose)
        return True
        
    except Exception as e:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        log(f"❌ Failed to save pickle: {e}", verbose)
        return False


def get_token_to_symbol_mapping(verbose: bool = True) -> Dict[int, str]:
    """
    Get instrument token to trading symbol mapping using KiteInstruments.
    
    This uses PKBrokers' KiteInstruments class to get the authoritative
    mapping between instrument tokens and trading symbols.
    
    Returns:
        Dict mapping instrument_token (int) to tradingsymbol (str)
    """
    global _token_to_symbol_cache
    
    if _token_to_symbol_cache:
        return _token_to_symbol_cache
    
    try:
        # Add parent directory to path for imports
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(os.path.dirname(script_dir))
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        
        from pkbrokers.kite.instruments import KiteInstruments
        from PKDevTools.classes.Environment import PKEnvironment
        
        env = PKEnvironment()
        instruments = KiteInstruments(
            api_key="kitefront",
            access_token=env.KTOKEN or "",
            local=True
        )
        
        # Get all equities with token and symbol
        equities = instruments.get_equities(
            column_names="instrument_token,tradingsymbol",
            only_nse_stocks=False
        )
        
        for eq in equities:
            token = eq.get('instrument_token')
            symbol = eq.get('tradingsymbol')
            if token and symbol:
                _token_to_symbol_cache[int(token)] = str(symbol)
        
        log(f"Loaded {len(_token_to_symbol_cache)} token-to-symbol mappings from KiteInstruments", verbose)
        
    except Exception as e:
        log(f"Warning: Could not load token mappings from KiteInstruments: {e}", verbose)
    
    return _token_to_symbol_cache


def map_tokens_to_symbols(data: Dict, verbose: bool = True) -> Dict:
    """
    Convert instrument token keys to trading symbol keys.
    
    Args:
        data: Dictionary that may have numeric keys (instrument tokens)
        verbose: Whether to log progress
        
    Returns:
        Dictionary with symbol keys instead of token keys where possible
    """
    token_to_symbol = get_token_to_symbol_mapping(verbose)
    
    if not token_to_symbol:
        log("Warning: No token-to-symbol mapping available", verbose)
        return data
    
    result = {}
    mapped_count = 0
    unmapped_count = 0
    
    for key, value in data.items():
        str_key = str(key)
        
        if str_key.isdigit():
            # This is an instrument token - try to map it
            token = int(str_key)
            symbol = token_to_symbol.get(token)
            
            if symbol:
                # Use symbol as key, but check for conflicts
                if symbol in result:
                    # Symbol already exists, keep the one with more recent data
                    existing = result[symbol]
                    new_df = value if isinstance(value, pd.DataFrame) else pd.DataFrame(value.get('data', []))
                    existing_df = existing if isinstance(existing, pd.DataFrame) else pd.DataFrame(existing.get('data', []))
                    
                    if len(new_df) > len(existing_df):
                        result[symbol] = value
                else:
                    result[symbol] = value
                mapped_count += 1
            else:
                # No mapping found - skip this entry
                unmapped_count += 1
        else:
            # Already a symbol key
            result[key] = value
    
    if mapped_count > 0:
        log(f"Mapped {mapped_count} instrument tokens to symbols", verbose)
    if unmapped_count > 0:
        log(f"Warning: Skipped {unmapped_count} unmapped instrument tokens", verbose)
    
    return result


def trim_daily_data_to_251_rows(data: Dict, verbose: bool = True) -> Dict:
    """
    Trim daily stock data to keep only the most recent 251 rows per stock.
    
    251 rows represents approximately 1 year of trading data (251 trading days
    per year in Indian markets). This ensures consistent file sizes and removes
    stale historical data.
    
    Also filters out numeric keys (instrument tokens) which have incomplete/stale data.
    
    Args:
        data: Dictionary mapping symbol to DataFrame or dict with 'data'/'index' keys
        verbose: Whether to log progress
        
    Returns:
        Trimmed data dictionary
    """
    MAX_ROWS = 251
    trimmed_count = 0
    removed_tokens = 0
    
    for symbol in list(data.keys()):
        # Remove numeric keys (instrument tokens) - they shouldn't be in the final output
        if str(symbol).isdigit():
            del data[symbol]
            removed_tokens += 1
            continue
            
        try:
            item = data[symbol]
            
            if isinstance(item, pd.DataFrame):
                if len(item) > MAX_ROWS:
                    # Sort by index to ensure we keep the most recent
                    item = item.sort_index()
                    data[symbol] = item.tail(MAX_ROWS)
                    trimmed_count += 1
            elif isinstance(item, dict):
                # Handle dict format with 'data' and 'index' keys
                if 'data' in item and 'index' in item:
                    if len(item['data']) > MAX_ROWS:
                        # Keep last MAX_ROWS entries
                        item['data'] = item['data'][-MAX_ROWS:]
                        item['index'] = item['index'][-MAX_ROWS:]
                        trimmed_count += 1
        except Exception as e:
            log(f"⚠️ Error trimming {symbol}: {e}", verbose)
            continue
    
    if removed_tokens > 0:
        log(f"🔄 Removed {removed_tokens} instrument tokens (unmapped)", verbose)
    if trimmed_count > 0:
        log(f"✂️ Trimmed {trimmed_count} stocks to {MAX_ROWS} rows each", verbose)
    
    return data


def get_last_trading_date(verbose: bool = True):
    """Get the last trading date using PKDateUtilities."""
    try:
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities
        last_trading = PKDateUtilities.tradingDate()
        if hasattr(last_trading, 'date'):
            return last_trading.date()
        return last_trading
    except Exception as e:
        log(f"⚠️ Could not get last trading date: {e}", verbose)
        return datetime.now().date()


def calculate_missing_trading_days(data: Dict, verbose: bool = True) -> int:
    """
    Calculate how many trading days are missing from the pkl data.
    
    Checks the actual date index in the data, not just the filename.
    
    Args:
        data: Dictionary containing stock data with datetime indices
        verbose: Whether to log progress
        
    Returns:
        Number of missing trading days (0 if current)
    """
    try:
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities
        
        if not data:
            return 10  # Default to 10 days if no data
        
        # Find the latest date in the data by checking actual index values
        latest_date = None
        sample_symbols = ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'BHARTIARTL']
        
        # Try known symbols first, then fall back to sampling
        symbols_to_check = [s for s in sample_symbols if s in data] or list(data.keys())[:50]
        
        for symbol in symbols_to_check:
            try:
                sym_data = data[symbol]
                
                # Handle dict format (with 'data', 'columns', 'index' keys)
                if isinstance(sym_data, dict) and 'index' in sym_data:
                    index_values = sym_data['index']
                    if index_values:
                        # Get the max date from the index
                        dates = pd.to_datetime(index_values, errors='coerce')
                        valid_dates = dates.dropna()
                        if len(valid_dates) > 0:
                            symbol_max = valid_dates.max()
                            if hasattr(symbol_max, 'date'):
                                symbol_max = symbol_max.date()
                            if latest_date is None or symbol_max > latest_date:
                                latest_date = symbol_max
                                log(f"📊 {symbol}: latest date in data = {latest_date}", verbose)
                
                # Handle DataFrame format
                elif hasattr(sym_data, 'index') and len(sym_data) > 0:
                    symbol_max = sym_data.index.max()
                    if hasattr(symbol_max, 'date'):
                        symbol_max = symbol_max.date()
                    elif isinstance(symbol_max, str):
                        symbol_max = pd.to_datetime(symbol_max).date()
                    
                    if latest_date is None or symbol_max > latest_date:
                        latest_date = symbol_max
                        log(f"📊 {symbol}: latest date in data = {latest_date}", verbose)
                        
            except Exception as e:
                continue
        
        if latest_date is None:
            log("⚠️ Could not determine latest date from pkl data", verbose)
            return 5  # Default
        
        # Get last trading date
        last_trading_date = get_last_trading_date(verbose)
        
        if latest_date >= last_trading_date:
            log(f"✅ Data date is current: latest={latest_date}, last_trading={last_trading_date}", verbose)
            # Note: Even when date is current, the data might be from early morning (e.g., 10:24 AM)
            # and missing market close data (3:30 PM). We return 0 but the caller should still
            # try to merge with DB data to get the complete day's data.
            return 0
        
        # Calculate missing trading days
        try:
            missing_days = PKDateUtilities.trading_days_between(latest_date, last_trading_date)
            log(f"📅 Data is stale: latest={latest_date}, last_trading={last_trading_date}, missing={missing_days} trading days", verbose)
            return missing_days
        except:
            # Fallback: simple calendar day difference
            diff = (last_trading_date - latest_date).days
            missing_days = max(1, diff // 2)  # Rough estimate: ~half are trading days
            log(f"📅 Data is stale: latest={latest_date}, missing ~{missing_days} trading days (estimated)", verbose)
            return missing_days
            
    except Exception as e:
        log(f"⚠️ Error calculating missing days: {e}", verbose)
        return 5  # Default


def download_historical_pkl(verbose: bool = True, past_offset: int = 30) -> Tuple[Optional[Dict], int]:
    """
    Download the most recent historical pkl from GitHub.
    
    Searches through the last `past_offset` days to find a valid PKL file with
    quality data (≥248 rows per sample stock). Validates data freshness and
    quality before accepting.
    
    Args:
        verbose: Whether to log progress
        past_offset: Number of days to look back for PKL files
        
    Returns:
        Tuple of (data_dict, missing_trading_days) or (None, 0) if not found
    """
    
    # Try multiple locations and date formats
    base_urls = [
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/actions-data-download/",
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/results/Data/",
    ]
    
    # Try last `past_offset` days
    today = datetime.now()
    # Look back up to 30 days to find a valid pkl file with quality data
    for days_back in range(past_offset if past_offset > 0 else 30):
        date_string = PKDateUtilities.nthPastTradingDateStringFromFutureDate(n=days_back, d1=today)
        check_date = datetime.strptime(date_string, "%Y-%m-%d").date()
        
        # Try different date formats
        date_formats = [
            check_date.strftime('%d%m%Y'),  # 25022026
        ]
        
        for base_url in base_urls:
            for date_str in date_formats:
                url = f"{base_url}stock_data_{date_str}.pkl"
                try:
                    log(f"Trying: {url}", verbose)
                    response = requests.get(url, timeout=30)
                    if response.status_code == 200 and len(response.content) > 1000000:  # > 1MB
                        data = pickle.loads(response.content)
                        if isinstance(data, dict) and len(data) > 100:
                            # Validate data quality - check rows per stock
                            sample_symbols = ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK']
                            min_rows = 248  # Increased from 100 to 248 for better quality (approx 248 is full year)
                            
                            rows_ok = False
                            for sym in sample_symbols:
                                if sym in data:
                                    sym_data = data[sym]
                                    if isinstance(sym_data, dict) and 'data' in sym_data:
                                        row_count = len(sym_data.get('data', []))
                                    elif hasattr(sym_data, '__len__'):
                                        row_count = len(sym_data)
                                    else:
                                        row_count = 0
                                    
                                    if row_count >= min_rows:
                                        rows_ok = True
                                        log(f"✅ Data quality check passed: {sym} has {row_count} rows", verbose)
                                        break
                                    else:
                                        log(f"⚠️ Data quality issue: {sym} has only {row_count} rows (need {min_rows}+)", verbose)
                            
                            if not rows_ok:
                                log(f"⚠️ Skipping {url} - insufficient historical data", verbose)
                                continue
                            
                            # Also validate that the actual data date is close to the filename date
                            actual_missing = calculate_missing_trading_days(data, verbose)
                            if actual_missing > 20:
                                log(f"⚠️ Skipping {url} - data too stale ({actual_missing} trading days behind)", verbose)
                                continue
                            
                            log(f"✅ Downloaded historical pkl: {len(data)} instruments, {len(response.content)/1024/1024:.1f} MB", verbose)
                            
                            return data, actual_missing
                except Exception as e:
                    continue
    
    log("❌ Could not download historical pkl from GitHub", verbose)
    return None, 0


def is_data_fresh(data: Dict, verbose: bool = True) -> bool:
    """
    Check if the ticks data is from today's trading date.
    
    Compares the latest timestamp in the data against today's trading date.
    
    Args:
        data: Ticks data dictionary
        verbose: Whether to log progress
        
    Returns:
        True if data contains today's trading date, False otherwise
    """
    if not data or not isinstance(data, dict):
        return False
    
    # Get today's trading date
    from PKDevTools.classes.PKDateUtilities import PKDateUtilities
    today_trading_date = PKDateUtilities.tradingDate()
    
    # Find the latest timestamp in the data
    latest_timestamp_str = None
    for instrument_token, instrument_data in data.items():
        last_updated = instrument_data.get("last_updated")
        if last_updated:
            if latest_timestamp_str is None or last_updated > latest_timestamp_str:
                latest_timestamp_str = last_updated
    
    if latest_timestamp_str:
        # Parse the timestamp (handling both with and without microseconds)
        try:
            # Try with microseconds first
            latest_date = datetime.fromisoformat(latest_timestamp_str).date()
        except ValueError:
            # Try without microseconds
            latest_date = datetime.fromisoformat(latest_timestamp_str.split('.')[0]).date()
        
        log(f"Latest data date: {latest_date}, Today's trading date: {today_trading_date}", verbose)

        # If the latest date in ticks.json is GREATER than today's trading date,
        # it means ticks.json contains data for a future or non-trading day that should be treated as fresh.
        # If the latest date in ticks.json is EQUAL to today's trading date, it's fresh.
        if latest_date >= today_trading_date:
            log(f"✅ Ticks.json latest date ({latest_date}) is on or after today's trading date ({today_trading_date}). Considered fresh.", verbose)
            return True
        
        # If latest_date is OLDER than today_trading_date, it's stale.
        log(f"⚠️ Ticks.json latest date ({latest_date}) is older than today's trading date ({today_trading_date}). Considered stale.", verbose)
        return False
    
    return False


def download_ticks_json(verbose: bool = True) -> Optional[Dict]:
    """Download ticks.json from GitHub with freshness check."""
    
    from PKDevTools.classes.PKDateUtilities import PKDateUtilities
    today_trading_date = PKDateUtilities.tradingDate()
    log(f"Looking for ticks from trading date: {today_trading_date}", verbose)
    
    urls = [
        # Primary location - actions-data-download subdirectory
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/actions-data-download/ticks.json.zip",
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/actions-data-download/ticks.json",
        # Fallback locations
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/results/Data/ticks.json.zip",
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/results/Data/ticks.json",
        "https://raw.githubusercontent.com/pkjmesra/PKBrokers/main/pkbrokers/kite/examples/results/Data/ticks.json",
    ]
    
    for url in urls:
        try:
            log(f"Trying ticks: {url}", verbose)
            response = requests.get(url, timeout=30)
            if response.status_code != 200:
                continue
            
            # Load data from response
            if url.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    if 'ticks.json' in zf.namelist():
                        with zf.open('ticks.json') as f:
                            data = json.load(f)
                    else:
                        continue
            else:
                data = response.json()
            
            # Validate and check freshness
            if isinstance(data, dict) and len(data) > 100:
                if is_data_fresh(data, verbose):
                    log(f"✅ Downloaded fresh ticks.json: {len(data)} instruments", verbose)
                    return data
                else:
                    log(f"⚠️ Downloaded ticks.json is stale (not from today)", verbose)
            else:
                log(f"⚠️ Downloaded ticks.json has insufficient data: {len(data) if isinstance(data, dict) else 0} instruments", verbose)
                
        except Exception as e:
            log(f"⚠️ Error with {url}: {e}", verbose)
            continue
    
    log("❌ Could not download fresh ticks.json from GitHub", verbose)
    return None


def load_local_ticks_json(data_dir: str, verbose: bool = True, min_instruments: int = 10) -> Optional[Dict]:
    """
    Load ticks.json from local path with freshness check.
    
    Args:
        data_dir: Directory to search for ticks.json
        verbose: Whether to log progress
        min_instruments: Minimum number of instruments required (default 10)
    
    Returns:
        Dict of fresh ticks data or None if not found/too small/stale
    """
    
    # Get today's trading date
    from PKDevTools.classes.PKDateUtilities import PKDateUtilities
    today_trading_date = PKDateUtilities.tradingDate()
    log(f"Today's trading date: {today_trading_date}", verbose)
    
    # Define paths to check
    paths = [
        os.path.join(data_dir, "ticks.json"),
        os.path.join(data_dir, "results", "Data", "ticks.json"),
        os.path.join(data_dir, "results", "Data", "ticks.json.zip"),
        "ticks.json",
        "results/Data/ticks.json",
    ]
    
    # First, try to load local file and check freshness
    for path in paths:
        if os.path.exists(path):
            try:
                file_size = os.path.getsize(path)
                if file_size < 100:  # Skip tiny files (empty or nearly empty)
                    log(f"⚠️ Skipping tiny ticks.json: {path} ({file_size} bytes)", verbose)
                    continue
                
                # Handle zip files
                if path.endswith('.zip'):
                    with zipfile.ZipFile(path, 'r') as zf:
                        if 'ticks.json' in zf.namelist():
                            with zf.open('ticks.json') as f:
                                data = json.load(f)
                        else:
                            continue
                else:
                    with open(path, 'r') as f:
                        data = json.load(f)
                
                # Validate data
                if not (isinstance(data, dict) and len(data) >= min_instruments):
                    log(f"⚠️ ticks.json has too few instruments: {len(data) if isinstance(data, dict) else 0}", verbose)
                    continue
                
                # Check freshness
                if is_data_fresh(data, verbose):
                    log(f"✅ Loaded fresh local ticks.json: {path} ({len(data)} instruments)", verbose)
                    return data
                else:
                    log(f"⚠️ Local ticks.json is stale (not from today). Will fetch fresh data.", verbose)
                    
            except Exception as e:
                log(f"⚠️ Error loading {path}: {e}", verbose)
                continue
    
    return None


def find_sqlite_database(verbose: bool = True) -> Optional[str]:
    """Find SQLite history database in common locations."""
    
    search_paths = [
        os.path.expanduser('~/.PKDevTools_userdata'),
        os.path.expanduser('~/.pkbrokers'),
        '.',
        'results/Data',
    ]
    
    db_names = ['instrument_history.db', 'kite_history.db', 'history.db']
    
    for search_dir in search_paths:
        if not os.path.exists(search_dir):
            continue
        for db_name in db_names:
            db_path = os.path.join(search_dir, db_name)
            if os.path.exists(db_path):
                log(f"✅ Found database: {db_path}", verbose)
                return db_path
        # Also search for any .db files with 'history' in name
        try:
            for f in os.listdir(search_dir):
                if f.endswith('.db') and 'history' in f.lower():
                    db_path = os.path.join(search_dir, f)
                    log(f"✅ Found database: {db_path}", verbose)
                    return db_path
        except:
            continue
    
    log("❌ No SQLite database found", verbose)
    return None

def is_after_market_hours(verbose: bool = True) -> bool:
    """Check if current time in IST is after market close (>=15:30)."""
    from PKDevTools.classes.PKDateUtilities import PKDateUtilities
    now = datetime.now(KOLKATA_TZ).time()
    market_close = datetime.strptime("15:30", "%H:%M").time()
    is_after = now >= market_close
    log(f"Market close check: now={now}, close=15:30, after={is_after}", verbose)
    return is_after


def is_db_current(db_path: str, verbose: bool = True) -> bool:
    """Check if the database contains daily data for today's trading date."""
    try:
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities
        conn = sqlite3.connect(db_path)
        today_trading = PKDateUtilities.tradingDate()
        today_str = today_trading.strftime('%Y-%m-%d')
        
        query = """
        SELECT COUNT(1) FROM instrument_history
        WHERE (interval in ('day','minute') OR interval IS NULL)
          AND date(timestamp) = ?
        LIMIT 1
        """
        cursor = conn.execute(query, (today_str,))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        log(f"⚠️ Could not check database freshness: {e}", verbose)
        return False


def find_and_check_current_db(verbose: bool = True) -> Optional[str]:
    """Find a database that contains daily data for today's trading date."""
    db_path = find_sqlite_database(verbose)
    if db_path and is_db_current(db_path, verbose):
        return db_path
    return None

def load_from_sqlite(db_path: str, verbose: bool = True) -> Dict:
    """Load daily candles from SQLite database."""
    
    candles = {}
    
    if not db_path or not os.path.exists(db_path):
        log(f"Database not found: {db_path}", verbose)
        return candles
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Check available tables
        tables_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
        table_list = tables_df['name'].tolist()
        log(f"Tables in database: {table_list}", verbose)
        
        df = pd.DataFrame()
        has_instruments_table = 'instruments' in table_list
        
        # Try to JOIN with instruments table to get tradingsymbols directly
        if has_instruments_table and 'instrument_history' in table_list:
            try:
                query = """
                SELECT ih.instrument_token, ih.timestamp, ih.open, ih.high, ih.low, ih.close, ih.volume,
                       i.tradingsymbol
                FROM instrument_history ih
                JOIN instruments i ON ih.instrument_token = i.instrument_token
                WHERE ih.interval in ('day') OR ih.interval IS NULL
                ORDER BY ih.instrument_token, ih.timestamp
                """
                df = pd.read_sql_query(query, conn)
                if len(df) > 0:
                    log(f"Loaded {len(df)} rows with JOINed tradingsymbols from instrument_history + instruments", verbose)
            except Exception as e:
                log(f"JOIN query failed: {e}, falling back to separate queries", verbose)
                df = pd.DataFrame()
        
        # Fall back to loading instrument_history without JOIN
        if len(df) == 0:
            for table_name in ['instrument_history', 'history', 'candles', 'daily_candles']:
                try:
                    query = f"""
                    SELECT instrument_token, timestamp, open, high, low, close, volume
                    FROM {table_name}
                    WHERE interval in ('day') OR interval IS NULL
                    ORDER BY instrument_token, timestamp
                    """
                    df = pd.read_sql_query(query, conn)
                    if len(df) > 0:
                        log(f"Loaded {len(df)} rows from {table_name}", verbose)
                        break
                except Exception as e:
                    continue
        
        if len(df) == 0:
            log("No data found in database tables", verbose)
            conn.close()
            return candles
        
        # Check if we have tradingsymbol column from JOIN
        has_tradingsymbol = 'tradingsymbol' in df.columns
        
        # If no tradingsymbol from JOIN, try to load symbol mapping from instruments table
        token_to_symbol = {}
        if not has_tradingsymbol and has_instruments_table:
            try:
                instruments_df = pd.read_sql_query(
                    "SELECT instrument_token, tradingsymbol FROM instruments",
                    conn
                )
                for _, row in instruments_df.iterrows():
                    token_to_symbol[row['instrument_token']] = row['tradingsymbol']
                log(f"Loaded {len(token_to_symbol)} symbol mappings from instruments table", verbose)
            except Exception as e:
                log(f"Could not load from instruments table: {e}", verbose)
        
        # If still no symbols, try loading from separate instrument_history.db file
        if not has_tradingsymbol and len(token_to_symbol) == 0:
            # instrument_history.db is typically in the same directory as instrument_history.db
            db_dir = os.path.dirname(db_path)
            instruments_db_paths = [
                os.path.join(db_dir, "instrument_history.db"),
                os.path.join(os.path.dirname(db_dir), "instrument_history.db"),
                os.path.join(os.getcwd(), "instrument_history.db"),
            ]
            # Also check common user data directories
            try:
                from PKDevTools.classes import Archiver
                instruments_db_paths.append(os.path.join(Archiver.get_user_data_dir(), "instrument_history.db"))
            except:
                pass
            
            for inst_db_path in instruments_db_paths:
                if os.path.exists(inst_db_path):
                    try:
                        inst_conn = sqlite3.connect(inst_db_path)
                        instruments_df = pd.read_sql_query(
                            "SELECT instrument_token, tradingsymbol FROM instruments",
                            inst_conn
                        )
                        for _, row in instruments_df.iterrows():
                            token_to_symbol[row['instrument_token']] = row['tradingsymbol']
                        inst_conn.close()
                        log(f"Loaded {len(token_to_symbol)} symbol mappings from {inst_db_path}", verbose)
                        break
                    except Exception as e:
                        log(f"Could not load from {inst_db_path}: {e}", verbose)
        
        # If still no symbols, try PKBrokers KiteInstruments
        if not has_tradingsymbol and len(token_to_symbol) == 0:
            try:
                sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
                from pkbrokers.kite.instruments import KiteInstruments
                from PKDevTools.classes.Environment import PKEnvironment
                
                env = PKEnvironment()
                instruments = KiteInstruments(
                    api_key="kitefront",
                    access_token=env.KTOKEN or "",
                    local=True
                )
                for inst in instruments.get_or_fetch_instrument_tokens(all_columns=True):
                    if isinstance(inst, dict):
                        token_to_symbol[inst.get('instrument_token')] = inst.get('tradingsymbol', str(inst.get('instrument_token')))
                log(f"Loaded {len(token_to_symbol)} symbol mappings from KiteInstruments", verbose)
            except Exception as e:
                log(f"Could not load symbol mapping from KiteInstruments: {e}", verbose)
                log("Using instrument tokens as symbol names", verbose)
        
        # Convert to pkl format
        for token, group in df.groupby('instrument_token'):
            # Get symbol from tradingsymbol column, or from mapping, or use token
            if has_tradingsymbol:
                symbol = group['tradingsymbol'].iloc[0]
                if pd.isna(symbol) or not symbol:
                    symbol = token_to_symbol.get(token, str(token))
            else:
                symbol = token_to_symbol.get(token, str(token))
            
            cols_to_use = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            group_df = group[cols_to_use].copy()
            # Parse timestamps and ensure tz-aware for consistent merging
            group_df['timestamp'] = pd.to_datetime(group_df['timestamp'], format='mixed', utc=True)
            group_df.set_index('timestamp', inplace=True)
            # Convert to Kolkata timezone
            if hasattr(group_df.index, 'tz') and group_df.index.tz is not None:
                group_df.index = group_df.index.tz_convert(KOLKATA_TZ)
            else:
                group_df.index = group_df.index.tz_localize(KOLKATA_TZ)
            group_df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            candles[symbol] = group_df
        
        conn.close()
        log(f"Converted {len(candles)} instruments from database", verbose)
        
    except Exception as e:
        log(f"Error loading from database: {e}", verbose)
    
    return candles


def convert_ticks_to_candles(ticks_data: Dict, verbose: bool = True) -> Dict:
    """Convert ticks.json format to DataFrame candle format."""
    
    candles = {}
    
    for token_str, tick_info in ticks_data.items():
        try:
            symbol = tick_info.get('trading_symbol', str(token_str))
            ohlcv = tick_info.get('ohlcv', {})
            
            if not ohlcv:
                continue
            
            close = float(ohlcv.get('close', 0))
            if close <= 0:
                continue
            
            # Parse timestamp and ensure tz-aware
            timestamp_str = ohlcv.get('timestamp', '')
            if timestamp_str:
                try:
                    dt = pd.to_datetime(timestamp_str, utc=True).tz_convert(KOLKATA_TZ)
                except:
                    dt = datetime.now(KOLKATA_TZ)
            else:
                dt = datetime.now(KOLKATA_TZ)
            
            # Create DataFrame with tz-aware index
            df = pd.DataFrame([{
                'Open': float(ohlcv.get('open', close)),
                'High': float(ohlcv.get('high', close)),
                'Low': float(ohlcv.get('low', close)),
                'Close': close,
                'Volume': int(ohlcv.get('volume', 0)),
            }], index=[dt])
            
            # Ensure index is tz-aware in Kolkata timezone
            df = _normalize_index_tz(df)
            
            candles[symbol] = df
            
        except Exception as e:
            continue
    
    log(f"Converted {len(candles)} instruments from ticks to candles", verbose)
    return candles


def _normalize_index_tz(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame index to timezone-aware IST for consistent merging.
    
    This ensures all DataFrames have timezone-aware indices in Asia/Kolkata
    timezone, allowing proper concatenation and comparison operations.
    """
    if df is None or not hasattr(df, 'index'):
        return df

    try:
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            # If timezone-aware, convert to Kolkata timezone
            df = df.copy()
            df.index = df.index.tz_convert(KOLKATA_TZ)
        else:
            # If timezone-naive, localize to Kolkata timezone
            df = df.copy()
            df.index = df.index.tz_localize(KOLKATA_TZ)
    except Exception:
        pass

    return df


def merge_candles(historical: Dict, today: Dict, verbose: bool = True) -> Dict:
    """
    Merge today's candles with historical data.
    
    Combines two dictionaries of DataFrames, removing duplicate timestamps
    (keeping the most recent) and ensuring consistent timezone handling.
    
    Args:
        historical: Base historical data dictionary
        today: New data to merge in
        verbose: Whether to log progress
        
    Returns:
        Merged dictionary
    """
    
    if not historical:
        log("No historical data to merge with", verbose)
        return today
    
    # First, map any instrument tokens to symbols in both datasets
    log("Mapping instrument tokens to symbols...", verbose)
    historical = map_tokens_to_symbols(historical, verbose)
    today = map_tokens_to_symbols(today, verbose)
    
    merged = {}
    today_date = datetime.now().date()
    
    # Start with all historical data (skip any remaining numeric keys)
    for symbol, hist_df in historical.items():
        # Skip numeric keys (instrument tokens) that couldn't be mapped
        if str(symbol).isdigit():
            continue
            
        if isinstance(hist_df, dict):
            # Convert dict format to DataFrame
            if 'data' in hist_df and 'columns' in hist_df:
                hist_df = pd.DataFrame(hist_df['data'], columns=hist_df['columns'])
                if 'index' in historical[symbol]:
                    hist_df.index = pd.to_datetime(historical[symbol]['index'], format='mixed')
        
        if hasattr(hist_df, 'index'):
            # Ensure timezone-aware
            hist_df = _normalize_index_tz(hist_df)
            merged[symbol] = hist_df.copy()
    
    # Add/update with today's data
    updated_count = 0
    new_count = 0
    skipped_tokens = 0
    
    for symbol, today_df in today.items():
        # Skip numeric keys (instrument tokens) that couldn't be mapped
        if str(symbol).isdigit():
            skipped_tokens += 1
            continue
            
        # Ensure today's data is also timezone-aware
        today_df = _normalize_index_tz(today_df)
        
        if symbol in merged:
            # Append today's data, removing duplicates
            existing = merged[symbol]
            # Ensure both are tz-aware before concatenation
            combined = pd.concat([existing, today_df])
            combined = combined[~combined.index.duplicated(keep='last')]
            combined = combined.sort_index()
            merged[symbol] = combined
            updated_count += 1
        else:
            merged[symbol] = today_df
            new_count += 1
    
    if skipped_tokens > 0:
        log(f"Warning: Skipped {skipped_tokens} unmappable instrument tokens", verbose)
    log(f"Merged: {updated_count} updated, {new_count} new, {len(merged)} total instruments", verbose)
    return merged


def trigger_history_download(missing_days: int, verbose: bool = True) -> bool:
    """
    Trigger the history download workflow via GitHub API.
    
    Args:
        missing_days: Number of trading days to fetch
        verbose: Whether to log progress
        
    Returns:
        True if workflow was triggered successfully
    """
    try:
        ci_pat = os.environ.get('CI_PAT') or os.environ.get('GITHUB_TOKEN')
        if not ci_pat:
            log("⚠️ No CI_PAT or GITHUB_TOKEN available to trigger workflow", verbose)
            return False
        
        url = "https://api.github.com/repos/pkjmesra/PKBrokers/actions/workflows/w1-workflow-history-data-child.yml/dispatches"
        
        headers = {
            "Authorization": f"token {ci_pat}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        payload = {
            "ref": "main",
            "inputs": {
                "period": "day",
                "pastoffset": str(missing_days),
                "logLevel": "20"
            }
        }
        
        log(f"🚀 Triggering history download workflow with pastoffset={missing_days}...", verbose)
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 204:
            log("✅ History download workflow triggered successfully", verbose)
            return True
        else:
            log(f"⚠️ Failed to trigger workflow: {response.status_code} - {response.text}", verbose)
            return False
            
    except Exception as e:
        log(f"⚠️ Error triggering history workflow: {e}", verbose)
        return False


def save_pkl_files(data: Dict, data_dir: str, verbose: bool = True) -> Tuple[str, str]:
    """
    Save pkl files with both generic and dated names using atomic writes.
    
    Daily data is trimmed to 251 rows per stock before saving.
    
    Args:
        data: Dictionary of stock data
        data_dir: Output directory
        verbose: Whether to log progress
        
    Returns:
        Tuple of (daily_path, generic_path) or ("", "") on failure
    """
    os.makedirs(data_dir, exist_ok=True)

    from PKDevTools.classes import Archiver
    _, file_name = Archiver.afterMarketStockDataExists()
    today_str = file_name.replace('.pkl','').replace('stock_data_','')
    
    # Trim daily data to 251 rows per stock before saving
    data = trim_daily_data_to_251_rows(data, verbose)
    
    # Save daily pkl using safe atomic write
    daily_path = os.path.join(data_dir, f"stock_data_{today_str}.pkl")
    if not safe_pickle_save(data, daily_path, verbose):
        log(f"❌ Failed to save daily pkl to {daily_path}", verbose)
        return "", ""
    
    daily_size = os.path.getsize(daily_path) / (1024 * 1024)
    log(f"✅ Saved daily pkl: {daily_path} ({daily_size:.2f} MB, {len(data)} instruments)", verbose)
    
    # Also save as generic name using atomic write
    generic_path = os.path.join(data_dir, "daily_candles.pkl")
    safe_pickle_save(data, generic_path, verbose)
    
    return daily_path, generic_path


def save_intraday_pkl(ticks_candles: Dict, data_dir: str, verbose: bool = True) -> str:
    """Save intraday pkl from today's ticks using atomic writes."""
    
    os.makedirs(data_dir, exist_ok=True)
    
    from PKDevTools.classes import Archiver
    _, file_name = Archiver.afterMarketStockDataExists()
    if file_name is not None and len(file_name) > 0:
        today = file_name.replace('.pkl','').replace('stock_data_','')
    else:
        # Fallback
        today = datetime.now().strftime('%d%m%Y')

    intraday_path = os.path.join(data_dir, f"intraday_stock_data_{today}.pkl")
    if not safe_pickle_save(ticks_candles, intraday_path, verbose):
        log(f"❌ Failed to save intraday pkl to {intraday_path}", verbose)
        return ""
    
    size = os.path.getsize(intraday_path) / (1024 * 1024)
    log(f"✅ Saved intraday pkl: {intraday_path} ({size:.2f} MB, {len(ticks_candles)} instruments)", verbose)
    
    # Also save as generic name
    generic_path = os.path.join(data_dir, "intraday_1m_candles.pkl")
    safe_pickle_save(ticks_candles, generic_path, verbose)
    
    return intraday_path

def load_from_sqlite_intraday(db_path: str, verbose: bool = True) -> Dict:
    """Load intraday candles from SQLite database for intervals other than 'day'."""
    
    candles = {}
    
    if not db_path or not os.path.exists(db_path):
        log(f"Database not found: {db_path}", verbose)
        return candles
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Get all intervals except 'day'
        query = """
        SELECT instrument_token, timestamp, open, high, low, close, volume, interval
        FROM instrument_history
        WHERE interval != 'day' AND interval IS NOT NULL
        ORDER BY instrument_token, timestamp
        """
        
        df = pd.read_sql_query(query, conn)
        
        if len(df) == 0:
            log("No intraday data found in database", verbose)
            conn.close()
            return candles
        
        # Load token-to-symbol mapping
        token_to_symbol = {}
        try:
            instruments_df = pd.read_sql_query(
                "SELECT instrument_token, tradingsymbol FROM instruments",
                conn
            )
            for _, row in instruments_df.iterrows():
                token_to_symbol[row['instrument_token']] = row['tradingsymbol']
            log(f"Loaded {len(token_to_symbol)} symbol mappings from instruments table", verbose)
        except Exception as e:
            log(f"Could not load from instruments table: {e}", verbose)
        
        # Group by instrument token
        for token, group in df.groupby('instrument_token'):
            symbol = token_to_symbol.get(token, str(token))
            
            # Sort by timestamp
            group = group.sort_values('timestamp')
            
            cols_to_use = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            group_df = group[cols_to_use].copy()
            group_df['timestamp'] = pd.to_datetime(group_df['timestamp'], format='mixed', utc=True)
            group_df.set_index('timestamp', inplace=True)
            
            if hasattr(group_df.index, 'tz') and group_df.index.tz is not None:
                group_df.index = group_df.index.tz_convert(KOLKATA_TZ)
            else:
                group_df.index = group_df.index.tz_localize(KOLKATA_TZ)
            
            group_df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            candles[symbol] = group_df
        
        conn.close()
        log(f"Converted {len(candles)} instruments from intraday database data", verbose)
        
    except Exception as e:
        log(f"Error loading intraday from database: {e}", verbose)
    
    return candles

def main():
    """Main entry point for the PKL generator."""
    parser = argparse.ArgumentParser(description="Generate pkl files from ticks.json or SQLite database")
    parser.add_argument("--data-dir", default="results/Data", help="Output directory for pkl files")
    parser.add_argument("--from-db", action="store_true", help="Load data from SQLite database instead of ticks.json")
    parser.add_argument("--db-only", action="store_true", help="Database-only mode: Use ONLY SQLite database, ignore GitHub PKL and ticks.json. Use this after market hours for fresh EOD data.")
    parser.add_argument("--db-path", default=None, help="Path to SQLite database (auto-detected if not specified)")
    parser.add_argument("--trigger-history", action="store_true", help="Trigger history download workflow if data is stale")
    parser.add_argument("--past-offset", default=30, help="Past offset days to check for the existence of pkl files")
    parser.add_argument("--verbose", "-v", action="store_true", default=True, help="Verbose output")
    parser.add_argument("--interval", default="day", help="Interval for which the pkl file has to be generated")
    args = parser.parse_args()
    
    verbose = args.verbose
    data_dir = args.data_dir
    past_offset = int(args.past_offset) if args.past_offset else 375
    data_interval = args.interval
    past_offset = 251 if data_interval == 'day' else 375
    log("=" * 60, verbose)
    log("PKL Generator: Unified pkl file generation", verbose)
    log(f"Mode: {'SQLite Database' if args.from_db else 'Ticks JSON'}", verbose)
    log(f"Past offset: {past_offset} days", verbose)
    log("=" * 60, verbose)
    
    new_candles = {}
    
    from pkbrokers.bot.dataSharingManager import get_data_sharing_manager
    from pkbrokers.kite.inMemoryCandleStore import get_candle_store
    from PKDevTools.classes.PKDateUtilities import PKDateUtilities

    data_mgr = get_data_sharing_manager()
    candle_store = get_candle_store()
    today_trading_date = PKDateUtilities.tradingDate()

    # Auto-detect fresh local database but use DB-only only after market hours
    if not args.from_db and not args.db_only:
        db_path = find_and_check_current_db(verbose)
        if db_path and is_after_market_hours(verbose):
            log("\n" + "=" * 60, verbose)
            log("🔄 AUTO-DETECTED: Local database contains today's data AND market is closed.", verbose)
            log("   Switching to DB-ONLY mode (ignoring GitHub PKL & ticks.json)", verbose)
            log("=" * 60, verbose)
            args.db_only = True
            args.db_path = db_path
        elif db_path:
            log("ℹ️ Found fresh local database but market is open – using normal mode (ticks.json + historical merge)", verbose)

    # =============================================================
    # DATABASE-ONLY MODE: Skip all merging, just use DB data
    # Use this after market hours for fresh EOD data
    # =============================================================
    if args.db_only:
        log("\n" + "=" * 60, verbose)
        log("🚀 DATABASE-ONLY MODE ACTIVE", verbose)
        log("   - Ignoring GitHub historical PKL files", verbose)
        log("   - Ignoring ticks.json files", verbose)
        log("   - Using ONLY the SQLite database", verbose)
        log("   - This is the recommended mode after market hours (3:30 PM IST)", verbose)
        log("=" * 60, verbose)
        
        # Find the database
        db_path = args.db_path if args.db_path else find_sqlite_database(verbose)
        if not db_path:
            log("❌ No database found in db-only mode!", verbose)
            sys.exit(1)
        
        if data_interval == 'day':
            log(f"\n[DB-ONLY MODE] Loading daily candles from: {db_path}", verbose)
            db_candles = load_from_sqlite(db_path, verbose)
            
            if not db_candles:
                log("❌ No daily data found in database!", verbose)
            else:
                # Save daily PKL files
                log(f"\n[DB-ONLY MODE] Saving {len(db_candles)} daily instruments...", verbose)
                save_pkl_files(db_candles, data_dir, verbose)
        
        # Also save intraday if available
        log("\n[DB-ONLY MODE] Checking for intraday data...", verbose)
        try:
            intraday_candles = load_from_sqlite_intraday(db_path, verbose)
            if intraday_candles and len(intraday_candles) >= 10:
                log(f"Saving {len(intraday_candles)} intraday instruments...", verbose)
                save_intraday_pkl(intraday_candles, data_dir, verbose)
            else:
                log("⚠️ No intraday data found in database (this is normal for EOD runs)", verbose)
        except Exception as e:
            log(f"⚠️ Could not load intraday data: {e}", verbose)
        
        log("\n" + "=" * 60, verbose)
        log("✅ SUCCESS: PKL files generated (Database-Only mode)", verbose)
        log("=" * 60, verbose)
        sys.exit(0)
    
    # Step 1: Always download historical pkl first (this is our base)
    log("\n[Step 1] Downloading historical pkl from GitHub...", verbose)
    historical_data, missing_trading_days = download_historical_pkl(verbose, past_offset=past_offset+1)
    
    # Step 1.5: Get current day's data from InMemoryCandleStore if available
    current_day_data_from_store = None
    if candle_store.get_all_instruments_ohlcv(interval='day'): # Check if store has any daily data
        log("Retrieving current day's aggregated data from InMemoryCandleStore...", verbose)
        # Export only today's candles from store, do not merge with other historical data here
        success, current_day_pkl_path = data_mgr.export_daily_candles_to_pkl(candle_store, merge_with_historical=False)
        if success and os.path.exists(current_day_pkl_path):
            with open(current_day_pkl_path, 'rb') as f:
                current_day_data_from_store = pickle.load(f)
            log(f"Retrieved {len(current_day_data_from_store)} symbols for today from InMemoryCandleStore.", verbose)
        else:
            log("No current day's aggregated data found in InMemoryCandleStore.", verbose)

    if historical_data:
        log(f"Historical data: {len(historical_data)} instruments", verbose)
        if missing_trading_days > 0:
            log(f"⚠️ Historical data is missing {missing_trading_days} trading days", verbose)
            
            # Trigger history download workflow if requested
            if args.trigger_history:
                trigger_history_download(missing_trading_days, verbose)
        else:
            log("✅ Historical data date is current", verbose)
            # NOTE: Even when the date is current, the data might be from early morning (e.g., 10:24 AM)
            # and missing market close data (3:30 PM). We return 0 but the caller should still
            # try to merge with DB data to get the complete day's data.
            
            # Optimization: If historical data is current, and it's a non-trading day or outside market hours,
            # we can skip loading new data entirely for daily candles.
            should_load_new_data = True
            current_system_date = datetime.now().date()
            
            is_today_system_date_a_trading_day = (PKDateUtilities.currentDateTime().date() == today_trading_date)

            if not is_today_system_date_a_trading_day: # It's a non-trading day (weekend/holiday)
                log("✅ Historical data is current and today is a non-trading day. Skipping loading new ticks/DB data.", verbose)
                should_load_new_data = False
            elif not PKDateUtilities.isTradingTime(): # It's a trading day, but not market hours
                log("✅ Historical data is current, it's a trading day, but outside market hours. Skipping loading new ticks/DB data.", verbose)
                should_load_new_data = False
            
            if not should_load_new_data:
                # Skip Step 2 and just save the historical data (which is already current)
                log("\n[Step 2] Skipping loading new data.", verbose)
                log("\n[Step 4] Saving historical pkl files (already current).", verbose)
                save_pkl_files(historical_data, data_dir, verbose)
                log("=" * 60, verbose)
                log("✅ SUCCESS: PKL files generated (from current historical data)", verbose)
                log("=" * 60, verbose)
                sys.exit(0) # Exit early, as nothing new needs to be processed
    else:
        log("⚠️ No historical pkl found on GitHub", verbose)
        missing_trading_days = 0
        
        # If no historical data and trigger is enabled, fetch last 10 days
        if args.trigger_history:
            trigger_history_download(10, verbose)
    
    # Merge current_day_data_from_store with historical_data
    if current_day_data_from_store:
        log("Merging current day's aggregated data with historical data...", verbose)
        if historical_data:
            historical_data = merge_candles(historical_data, current_day_data_from_store, verbose)
        else:
            historical_data = current_day_data_from_store

    # Step 2: Load new data based on mode
    log("\n[Step 2] Loading new data...", verbose)
    
    # The subsequent logic should now merge with the potentially updated `historical_data`
    # or save `new_candles` if `historical_data` is still empty.
    # The core change is ensuring `historical_data` already contains current day's ticks if available.

    if args.from_db:
        # Load from SQLite database
        db_path = args.db_path if args.db_path else find_sqlite_database(verbose)
        if db_path:
            new_candles = load_from_sqlite(db_path, verbose)
        else:
            log("⚠️ No database found, trying ticks.json as fallback...", verbose)
            ticks_data = load_local_ticks_json(data_dir, verbose)
            if ticks_data:
                new_candles = convert_ticks_to_candles(ticks_data, verbose)

        # Merge with historical data
        if historical_data and new_candles:
            log("Merging database/ticks data with historical data...", verbose)
            merged_data = merge_candles(historical_data, new_candles, verbose)
            log("\n[Step 4] Saving merged pkl files...", verbose)
            save_pkl_files(merged_data, data_dir, verbose)
        elif new_candles:
            log("No historical data to merge with, saving new_candles data only", verbose)
            save_pkl_files(new_candles, data_dir, verbose)
        elif historical_data:
            log("No new data from database, saving historical data only", verbose)
            save_pkl_files(historical_data, data_dir, verbose)
        else:
            log("❌ FAILED: No data available (neither historical nor new)", verbose)
            sys.exit(1)
    else:
        # Load from ticks.json
        ticks_data = load_local_ticks_json(data_dir, verbose)
        if not ticks_data:
            ticks_data = download_ticks_json(verbose)
        
        if ticks_data:
            new_candles = convert_ticks_to_candles(ticks_data, verbose)
            
            # Save intraday pkl (just today's ticks) - only if we have meaningful data
            if new_candles and len(new_candles) >= 10:
                save_intraday_pkl(new_candles, data_dir, verbose)
            else:
                log("⚠️ Not enough new data for intraday pkl", verbose)
            
            # Merge with historical data for daily pkl
            if historical_data:
                merged_data = merge_candles(historical_data, new_candles, verbose)
                log("\n[Step 4] Saving merged daily pkl files...", verbose)
                save_pkl_files(merged_data, data_dir, verbose)
            else:
                log("No historical data to merge with, saving ticks data only", verbose)
                save_pkl_files(new_candles, data_dir, verbose)
        else:
            log("⚠️ No ticks data found, using historical data only", verbose)
            if historical_data:
                save_pkl_files(historical_data, data_dir, verbose)
            else:
                log("❌ FAILED: No data available (neither historical nor new)", verbose)
                sys.exit(1)
    
    log("=" * 60, verbose)
    log("✅ SUCCESS: PKL files generated", verbose)
    log("=" * 60, verbose)


if __name__ == "__main__":
    main()