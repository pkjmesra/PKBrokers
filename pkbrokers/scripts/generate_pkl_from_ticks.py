#!/usr/bin/env python3
"""
Simple, reliable script to generate pkl files from ticks.json.

This script:
1. Downloads existing historical pkl from GitHub (if available)
2. Loads ticks.json (local or from GitHub)
3. Converts ticks to candle format
4. Merges with historical data
5. Saves as dated pkl files

Usage:
    python generate_pkl_from_ticks.py [--data-dir PATH] [--verbose]
"""

import argparse
import io
import json
import os
import pickle
import sys
import zipfile
from datetime import datetime
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
    parser = argparse.ArgumentParser(description="Generate pkl files from ticks.json")
    parser.add_argument("--data-dir", default="results/Data", help="Output directory for pkl files")
    parser.add_argument("--verbose", "-v", action="store_true", default=True, help="Verbose output")
    args = parser.parse_args()
    
    verbose = args.verbose
    data_dir = args.data_dir
    
    log("=" * 60, verbose)
    log("PKL Generator: Converting ticks to pkl files", verbose)
    log("=" * 60, verbose)
    
    # Step 1: Load ticks.json (local first, then GitHub)
    ticks_data = load_local_ticks_json(data_dir, verbose)
    if not ticks_data:
        ticks_data = download_ticks_json(verbose)
    
    if not ticks_data:
        log("❌ FAILED: No ticks.json available", verbose)
        sys.exit(1)
    
    # Step 2: Convert ticks to candle format
    ticks_candles = convert_ticks_to_candles(ticks_data, verbose)
    
    if not ticks_candles:
        log("❌ FAILED: Could not convert ticks to candles", verbose)
        sys.exit(1)
    
    # Step 3: Save intraday pkl (just today's ticks)
    save_intraday_pkl(ticks_candles, data_dir, verbose)
    
    # Step 4: Download historical pkl from GitHub
    historical_data = download_historical_pkl(verbose)
    
    # Step 5: Merge today's candles with historical
    merged_data = merge_candles(historical_data, ticks_candles, verbose)
    
    # Step 6: Save merged daily pkl
    save_pkl_files(merged_data, data_dir, verbose)
    
    log("=" * 60, verbose)
    log("✅ SUCCESS: PKL files generated", verbose)
    log("=" * 60, verbose)


if __name__ == "__main__":
    main()

