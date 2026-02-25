#!/usr/bin/env python3
"""
Unified PKL generator for PKBrokers - handles both ticks.json and SQLite sources.

This script:
1. Downloads existing historical pkl from GitHub (~37MB)
2. Loads new data from ticks.json OR SQLite database
3. Converts to candle format
4. Merges with historical data
5. Saves as dated pkl files (~37MB+)

Usage:
    # From ticks.json (default - for market hours with tick merging)
    python generate_pkl_from_ticks.py [--data-dir PATH] [--verbose]
    
    # From SQLite database (for after-market hours, no tick merging)
    python generate_pkl_from_ticks.py --from-db [--db-path PATH] [--data-dir PATH] [--verbose]
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
import requests

from pkbrokers.kite.examples.pkkite import kite_ticks


def log(msg: str, verbose: bool = True):
    if verbose:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# Global cache for token-to-symbol mapping
_token_to_symbol_cache: Dict[int, str] = {}


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
    
    This ensures consistent file sizes and keeps approximately 1 year of trading data.
    Only applies to daily data (stock_data_*.pkl), not intraday data.
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
    """Calculate how many trading days are missing from the pkl data.
    
    Checks the actual date index in the data, not just the filename.
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
                        dates = pd.to_datetime(index_values, format='mixed', errors='coerce')
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
    """Download the most recent historical pkl from GitHub.
    
    Returns:
        Tuple of (data_dict, missing_trading_days)
    """
    
    # Try multiple locations and date formats
    base_urls = [
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/actions-data-download/",
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/results/Data/",
    ]
    
    # Try last 30 days
    today = datetime.now()
    # Look back up to 30 days to find a valid pkl file with quality data
    for days_back in range(past_offset if past_offset > 0 else 30):
        check_date = today - timedelta(days=days_back)
        
        # Try different date formats
        date_formats = [
            check_date.strftime('%d%m%Y'),  # 25022026
            # check_date.strftime('%d%m%y'),   # 250226
            # f"{check_date.day}{check_date.strftime('%m%y')}",  # 250226 without leading zero
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
                            # If data is too old (> 20 trading days behind), skip and look for better file
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
    """Check if the ticks data is from today's trading date"""
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
        return latest_date == today_trading_date
    
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
    """Load ticks.json from local path with freshness check.
    
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
        os.path.join(data_dir, "results", "Data", "ticks.json.zip"),  # Also check for zip
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
    
    # If no fresh local data found, try generating fresh data with kite_ticks
    log("🔄 No fresh local data found. Attempting to generate fresh ticks with kite_ticks()...", verbose)
    try:
        # Call kite_ticks in test mode - 3 minutes is enough for a sample
        log("Running kite_ticks(test_mode=True) to generate fresh data...", verbose)
        from pkbrokers.kite.examples.pkkite import kite_ticks
        kite_ticks(test_mode=True)  # Keep as test_mode=True as requested
        
        # After kite_ticks completes, check the expected output location
        expected_path = os.path.join(data_dir, "results", "Data", "ticks.json")
        
        # Also check current directory as fallback
        possible_paths = [
            expected_path,
            os.path.join("results", "Data", "ticks.json"),
            "ticks.json"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                log(f"Checking for fresh data at: {path}", verbose)
                with open(path, 'r') as f:
                    data = json.load(f)
                
                if isinstance(data, dict) and len(data) >= min_instruments:
                    if is_data_fresh(data, verbose):
                        log(f"✅ Successfully generated fresh ticks.json: {path} ({len(data)} instruments)", verbose)
                        return data
                    else:
                        log(f"⚠️ Generated ticks.json is stale - something went wrong", verbose)
                else:
                    log(f"⚠️ Generated ticks.json has insufficient data", verbose)
        
        log("❌ Could not find valid ticks.json after kite_ticks", verbose)
        
    except Exception as e:
        log(f"❌ Failed to generate fresh ticks via kite_ticks: {e}", verbose)
    
    # Final fallback: try to download from GitHub
    log("Attempting to download fresh ticks from GitHub as final fallback...", verbose)
    github_data = download_ticks_json(verbose)
    if github_data:
        return github_data
    
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
                WHERE ih.interval = 'day' OR ih.interval IS NULL
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
                    WHERE interval = 'day' OR interval IS NULL
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
        
        # If still no symbols, try loading from separate instruments.db file
        if not has_tradingsymbol and len(token_to_symbol) == 0:
            # instruments.db is typically in the same directory as instrument_history.db
            db_dir = os.path.dirname(db_path)
            instruments_db_paths = [
                os.path.join(db_dir, "instruments.db"),
                os.path.join(os.path.dirname(db_dir), "instruments.db"),
                os.path.join(os.getcwd(), "instruments.db"),
            ]
            # Also check common user data directories
            try:
                from PKDevTools.classes import Archiver
                instruments_db_paths.append(os.path.join(Archiver.get_user_data_dir(), "instruments.db"))
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
            # Parse timestamps and ensure tz-naive for consistent merging
            group_df['timestamp'] = pd.to_datetime(group_df['timestamp'], format='mixed', utc=True)
            group_df.set_index('timestamp', inplace=True)
            # Convert to tz-naive
            if hasattr(group_df.index, 'tz') and group_df.index.tz is not None:
                group_df.index = group_df.index.tz_localize(None)
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
            
            # Parse timestamp and ensure tz-naive
            timestamp_str = ohlcv.get('timestamp', '')
            if timestamp_str:
                try:
                    dt = pd.to_datetime(timestamp_str, utc=True)
                    # Convert to tz-naive
                    if hasattr(dt, 'tz') and dt.tz is not None:
                        dt = dt.tz_localize(None)
                except:
                    dt = datetime.now()
            else:
                dt = datetime.now()
            
            # Create DataFrame with tz-naive index
            df = pd.DataFrame([{
                'Open': float(ohlcv.get('open', close)),
                'High': float(ohlcv.get('high', close)),
                'Low': float(ohlcv.get('low', close)),
                'Close': close,
                'Volume': int(ohlcv.get('volume', 0)),
            }], index=[dt])
            
            # Ensure index is tz-naive
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            
            candles[symbol] = df
            
        except Exception as e:
            continue
    
    log(f"Converted {len(candles)} instruments from ticks to candles", verbose)
    return candles


def _normalize_index_tz(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame index to timezone-naive for consistent merging."""
    if df is None or not hasattr(df, 'index'):
        return df
    
    try:
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            # Convert tz-aware to tz-naive (UTC first, then remove tz)
            df = df.copy()
            df.index = df.index.tz_convert('UTC').tz_localize(None)
    except Exception:
        pass
    
    return df


def merge_candles(historical: Dict, today: Dict, verbose: bool = True) -> Dict:
    """Merge today's candles with historical data."""
    
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
                    # Use format='mixed' to handle various ISO8601 formats with timezone/microseconds
                    hist_df.index = pd.to_datetime(historical[symbol]['index'], format='mixed', utc=True)
                    # Convert to timezone-naive for consistency
                    hist_df.index = hist_df.index.tz_localize(None)
        
        if hasattr(hist_df, 'index'):
            # Ensure timezone-naive
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
            
        # Ensure today's data is also timezone-naive
        today_df = _normalize_index_tz(today_df)
        
        if symbol in merged:
            # Append today's data, removing duplicates
            existing = merged[symbol]
            # Ensure both are tz-naive before concatenation
            existing = _normalize_index_tz(existing)
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
    """Trigger the history download workflow via GitHub API.
    
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
    """Save pkl files with both generic and dated names.
    
    Daily data is trimmed to 251 rows per stock before saving.
    """
    
    os.makedirs(data_dir, exist_ok=True)
    
    today = datetime.now().strftime('%d%m%Y')
    
    # Trim daily data to 251 rows per stock before saving
    data = trim_daily_data_to_251_rows(data, verbose)
    
    # Save daily pkl
    daily_path = os.path.join(data_dir, f"stock_data_{today}.pkl")
    with open(daily_path, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    daily_size = os.path.getsize(daily_path) / (1024 * 1024)
    log(f"✅ Saved daily pkl: {daily_path} ({daily_size:.2f} MB, {len(data)} instruments)", verbose)
    
    # Also save as generic name
    generic_path = os.path.join(data_dir, "daily_candles.pkl")
    with open(generic_path, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    return daily_path, generic_path


def save_intraday_pkl(ticks_candles: Dict, data_dir: str, verbose: bool = True) -> str:
    """Save intraday pkl from today's ticks."""
    
    os.makedirs(data_dir, exist_ok=True)
    
    today = datetime.now().strftime('%d%m%Y')
    
    intraday_path = os.path.join(data_dir, f"intraday_stock_data_{today}.pkl")
    with open(intraday_path, 'wb') as f:
        pickle.dump(ticks_candles, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    size = os.path.getsize(intraday_path) / (1024 * 1024)
    log(f"✅ Saved intraday pkl: {intraday_path} ({size:.2f} MB, {len(ticks_candles)} instruments)", verbose)
    
    # Also save as generic name
    generic_path = os.path.join(data_dir, "intraday_1m_candles.pkl")
    with open(generic_path, 'wb') as f:
        pickle.dump(ticks_candles, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    return intraday_path


def main():
    parser = argparse.ArgumentParser(description="Generate pkl files from ticks.json or SQLite database")
    parser.add_argument("--data-dir", default="results/Data", help="Output directory for pkl files")
    parser.add_argument("--from-db", action="store_true", help="Load data from SQLite database instead of ticks.json")
    parser.add_argument("--db-path", default=None, help="Path to SQLite database (auto-detected if not specified)")
    parser.add_argument("--trigger-history", action="store_true", help="Trigger history download workflow if data is stale")
    parser.add_argument("--past-offset", default=30, help="Past offset days to check for the existence of pkl files")
    parser.add_argument("--verbose", "-v", action="store_true", default=True, help="Verbose output")
    args = parser.parse_args()
    
    verbose = args.verbose
    data_dir = args.data_dir
    past_offset = int(args.past_offset) if args.past_offset else 30
    
    log("=" * 60, verbose)
    log("PKL Generator: Unified pkl file generation", verbose)
    log(f"Mode: {'SQLite Database' if args.from_db else 'Ticks JSON'}", verbose)
    log(f"Past offset: {past_offset} days", verbose)
    log("=" * 60, verbose)
    
    new_candles = {}
    
    # Step 1: Always download historical pkl first (this is our base)
    log("\n[Step 1] Downloading historical pkl from GitHub...", verbose)
    historical_data, missing_trading_days = download_historical_pkl(verbose, past_offset=past_offset+1)
    
    if historical_data:
        log(f"Historical data: {len(historical_data)} instruments", verbose)
        if missing_trading_days > 0:
            log(f"⚠️ Historical data is missing {missing_trading_days} trading days", verbose)
            
            # Trigger history download workflow if requested
            if args.trigger_history:
                trigger_history_download(missing_trading_days, verbose)
        else:
            log("✅ Historical data date is current", verbose)
            # NOTE: Even when the date is current, we should still try to merge with DB data
            # because the pkl might have partial day data (e.g., 10:24 AM) while DB has
            # complete day data (e.g., 3:30 PM market close). Continue to Step 2.
    else:
        log("⚠️ No historical pkl found on GitHub", verbose)
        missing_trading_days = 0
        
        # If no historical data and trigger is enabled, fetch last 10 days
        if args.trigger_history:
            trigger_history_download(10, verbose)
    
    # Step 2: Load new data based on mode
    log("\n[Step 2] Loading new data...", verbose)
    
    if args.from_db:
        # Load from SQLite database
        db_path = args.db_path if args.db_path else find_sqlite_database(verbose)
        if db_path:
            new_candles = load_from_sqlite(db_path, verbose)
            
            # FIX: Merge database data with historical data
            if historical_data and new_candles:
                log("Merging database data with historical data...", verbose)
                merged_data = merge_candles(historical_data, new_candles, verbose)
                # Save merged data instead of just database data
                log("\n[Step 4] Saving merged pkl files from database + historical...", verbose)
                save_pkl_files(merged_data, data_dir, verbose)
            elif new_candles:
                log("No historical data to merge with, saving database data only", verbose)
                save_pkl_files(new_candles, data_dir, verbose)
            else:
                log("⚠️ No data from database, trying ticks.json as fallback...", verbose)
                ticks_data = load_local_ticks_json(data_dir, verbose)
                if ticks_data:
                    new_candles = convert_ticks_to_candles(ticks_data, verbose)
                    if historical_data:
                        merged_data = merge_candles(historical_data, new_candles, verbose)
                        save_pkl_files(merged_data, data_dir, verbose)
                    else:
                        save_pkl_files(new_candles, data_dir, verbose)
        else:
            log("⚠️ No database found, trying ticks.json as fallback...", verbose)
            ticks_data = load_local_ticks_json(data_dir, verbose)
            if ticks_data:
                new_candles = convert_ticks_to_candles(ticks_data, verbose)
                if historical_data:
                    merged_data = merge_candles(historical_data, new_candles, verbose)
                    save_pkl_files(merged_data, data_dir, verbose)
                else:
                    save_pkl_files(new_candles, data_dir, verbose)
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