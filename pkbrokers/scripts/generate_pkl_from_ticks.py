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
    # From ticks.json (default)
    python generate_pkl_from_ticks.py [--data-dir PATH] [--verbose]
    
    # From SQLite database (for history workflow)
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


def log(msg: str, verbose: bool = True):
    if verbose:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def download_historical_pkl(verbose: bool = True) -> Optional[Dict]:
    """Download the most recent historical pkl from GitHub."""
    
    # Try multiple locations and date formats
    base_urls = [
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/actions-data-download/",
        "https://raw.githubusercontent.com/pkjmesra/PKScreener/actions-data-download/results/Data/",
    ]
    
    # Try last 10 days
    today = datetime.now()
    for days_back in range(10):
        check_date = today - pd.Timedelta(days=days_back)
        
        # Try different date formats
        date_formats = [
            check_date.strftime('%d%m%Y'),  # 29122025
            check_date.strftime('%d%m%y'),   # 291225
            f"{check_date.day}{check_date.strftime('%m%y')}",  # 291225 without leading zero
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
                            log(f"✅ Downloaded historical pkl: {len(data)} instruments, {len(response.content)/1024/1024:.1f} MB", verbose)
                            return data
                except Exception as e:
                    continue
    
    log("❌ Could not download historical pkl from GitHub", verbose)
    return None


def download_ticks_json(verbose: bool = True) -> Optional[Dict]:
    """Download ticks.json from GitHub."""
    
    urls = [
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
            
            if url.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    if 'ticks.json' in zf.namelist():
                        with zf.open('ticks.json') as f:
                            data = json.load(f)
                    else:
                        continue
            else:
                data = response.json()
            
            if isinstance(data, dict) and len(data) > 100:
                log(f"✅ Downloaded ticks.json: {len(data)} instruments", verbose)
                return data
        except Exception as e:
            continue
    
    log("❌ Could not download ticks.json from GitHub", verbose)
    return None


def load_local_ticks_json(data_dir: str, verbose: bool = True) -> Optional[Dict]:
    """Load ticks.json from local path."""
    
    paths = [
        os.path.join(data_dir, "ticks.json"),
        os.path.join(data_dir, "results", "Data", "ticks.json"),
        "ticks.json",
        "results/Data/ticks.json",
    ]
    
    for path in paths:
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                if isinstance(data, dict) and len(data) > 0:
                    log(f"✅ Loaded local ticks.json: {path} ({len(data)} instruments)", verbose)
                    return data
            except Exception as e:
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
        log(f"Tables in database: {tables_df['name'].tolist()}", verbose)
        
        # Try different table names
        df = pd.DataFrame()
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
        
        # Load symbol mapping
        token_to_symbol = {}
        try:
            # Try to get symbol mapping from PKBrokers
            sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            from pkbrokers.kite.instruments import KiteInstruments
            instruments = KiteInstruments()
            for inst in instruments.get_or_fetch_instrument_tokens(all_columns=True):
                if isinstance(inst, dict):
                    token_to_symbol[inst.get('instrument_token')] = inst.get('tradingsymbol', str(inst.get('instrument_token')))
            log(f"Loaded {len(token_to_symbol)} symbol mappings", verbose)
        except Exception as e:
            log(f"Could not load symbol mapping: {e}", verbose)
        
        # Convert to pkl format
        for token, group in df.groupby('instrument_token'):
            symbol = token_to_symbol.get(token, str(token))
            group_df = group[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
            group_df['timestamp'] = pd.to_datetime(group_df['timestamp'])
            group_df.set_index('timestamp', inplace=True)
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
            
            # Parse timestamp
            timestamp_str = ohlcv.get('timestamp', '')
            if timestamp_str:
                try:
                    dt = pd.to_datetime(timestamp_str)
                except:
                    dt = datetime.now()
            else:
                dt = datetime.now()
            
            # Create DataFrame
            df = pd.DataFrame([{
                'Open': float(ohlcv.get('open', close)),
                'High': float(ohlcv.get('high', close)),
                'Low': float(ohlcv.get('low', close)),
                'Close': close,
                'Volume': int(ohlcv.get('volume', 0)),
            }], index=[dt])
            
            candles[symbol] = df
            
        except Exception as e:
            continue
    
    log(f"Converted {len(candles)} instruments from ticks to candles", verbose)
    return candles


def merge_candles(historical: Dict, today: Dict, verbose: bool = True) -> Dict:
    """Merge today's candles with historical data."""
    
    if not historical:
        log("No historical data to merge with", verbose)
        return today
    
    merged = {}
    today_date = datetime.now().date()
    
    # Start with all historical data
    for symbol, hist_df in historical.items():
        if isinstance(hist_df, dict):
            # Convert dict format to DataFrame
            if 'data' in hist_df and 'columns' in hist_df:
                hist_df = pd.DataFrame(hist_df['data'], columns=hist_df['columns'])
                if 'index' in historical[symbol]:
                    hist_df.index = pd.to_datetime(historical[symbol]['index'])
        
        if hasattr(hist_df, 'index'):
            merged[symbol] = hist_df.copy()
    
    # Add/update with today's data
    updated_count = 0
    new_count = 0
    
    for symbol, today_df in today.items():
        if symbol in merged:
            # Append today's data, removing duplicates
            existing = merged[symbol]
            combined = pd.concat([existing, today_df])
            combined = combined[~combined.index.duplicated(keep='last')]
            combined = combined.sort_index()
            merged[symbol] = combined
            updated_count += 1
        else:
            merged[symbol] = today_df
            new_count += 1
    
    log(f"Merged: {updated_count} updated, {new_count} new, {len(merged)} total instruments", verbose)
    return merged


def save_pkl_files(data: Dict, data_dir: str, verbose: bool = True) -> Tuple[str, str]:
    """Save pkl files with both generic and dated names."""
    
    os.makedirs(data_dir, exist_ok=True)
    
    today = datetime.now().strftime('%d%m%Y')
    
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
    parser.add_argument("--verbose", "-v", action="store_true", default=True, help="Verbose output")
    args = parser.parse_args()
    
    verbose = args.verbose
    data_dir = args.data_dir
    
    log("=" * 60, verbose)
    log("PKL Generator: Unified pkl file generation", verbose)
    log(f"Mode: {'SQLite Database' if args.from_db else 'Ticks JSON'}", verbose)
    log("=" * 60, verbose)
    
    new_candles = {}
    
    if args.from_db:
        # Load from SQLite database
        db_path = args.db_path if args.db_path else find_sqlite_database(verbose)
        if db_path:
            new_candles = load_from_sqlite(db_path, verbose)
        
        if not new_candles:
            log("⚠️ No data from database, trying ticks.json as fallback...", verbose)
            ticks_data = load_local_ticks_json(data_dir, verbose)
            if ticks_data:
                new_candles = convert_ticks_to_candles(ticks_data, verbose)
    else:
        # Load from ticks.json
        ticks_data = load_local_ticks_json(data_dir, verbose)
        if not ticks_data:
            ticks_data = download_ticks_json(verbose)
        
        if ticks_data:
            new_candles = convert_ticks_to_candles(ticks_data, verbose)
            # Save intraday pkl (just today's ticks)
            if new_candles:
                save_intraday_pkl(new_candles, data_dir, verbose)
    
    if not new_candles:
        log("⚠️ No new data available, downloading historical pkl only...", verbose)
    
    # Download historical pkl from GitHub
    historical_data = download_historical_pkl(verbose)
    
    if not historical_data and not new_candles:
        log("❌ FAILED: No data available (neither historical nor new)", verbose)
        sys.exit(1)
    
    # Merge today's candles with historical
    if new_candles:
        merged_data = merge_candles(historical_data, new_candles, verbose)
    else:
        merged_data = historical_data
        log("Using historical data only (no new data to merge)", verbose)
    
    # Save merged daily pkl
    save_pkl_files(merged_data, data_dir, verbose)
    
    log("=" * 60, verbose)
    log("✅ SUCCESS: PKL files generated", verbose)
    log("=" * 60, verbose)


if __name__ == "__main__":
    main()

