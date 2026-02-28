#!/usr/bin/env python
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

Publish Candle Data to GitHub Repository
=========================================

This script collects candle data from InMemoryCandleStore and publishes
it to a GitHub repository for consumption by scan workflows.

Usage:
    python publish_candle_data.py --output-dir /path/to/data

The script creates the following structure:
    data/
    ├── candles/
    │   ├── 2024-01-15/
    │   │   ├── candles_0915.json.gz
    │   │   ├── candles_0920.json.gz
    │   │   └── ...
    │   └── latest.json.gz
    ├── ticks/
    │   └── ticks_latest.json.gz
    └── metadata.json
"""

import argparse
import gzip
import json
import os
import pickle
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

# Ensure proper imports
try:
    from PKDevTools.classes import Archiver
    from PKDevTools.classes.log import default_logger
except ImportError:
    # Fallback for standalone execution
    class DummyLogger:
        def info(self, msg): print(f"[INFO] {msg}")
        def warning(self, msg): print(f"[WARN] {msg}")
        def error(self, msg): print(f"[ERROR] {msg}")
    
    def default_logger():
        return DummyLogger()


logger = default_logger()


def collect_from_candle_store() -> Optional[Dict[str, Any]]:
    """
    Collect candle data from InMemoryCandleStore.
    
    Returns:
        Dictionary with candle data for all instruments
    """
    try:
        from pkbrokers.kite import get_candle_store
        
        store = get_candle_store()
        stats = store.get_stats()
        
        if stats.get('instrument_count', 0) == 0:
            logger.warning("InMemoryCandleStore has no instruments")
            return None
        
        all_data = {}
        
        for token in store.instruments:
            symbol = store.instrument_symbols.get(token, str(token))
            
            instrument_data = {
                "token": token,
                "symbol": symbol,
                "candles": {}
            }
            
            # Export each supported interval
            for interval in ["1m", "2m", "3m", "4m", "5m", "10m", "15m", "30m", "60m", "day"]:
                try:
                    candles = store.get_candles(
                        instrument_token=token,
                        interval=interval,
                        count=500  # Store up to 500 candles per interval
                    )
                    if candles:
                        instrument_data["candles"][interval] = candles
                except Exception as e:
                    logger.warning(f"Error getting {interval} candles for {symbol}: {e}")
            
            # Add day summary OHLCV
            day_ohlcv = store.get_day_ohlcv(instrument_token=token)
            if day_ohlcv:
                instrument_data["day_ohlcv"] = day_ohlcv
            
            all_data[symbol] = instrument_data
        
        logger.info(f"Collected data for {len(all_data)} instruments from candle store")
        return all_data
        
    except ImportError as e:
        logger.warning(f"PKBrokers not available: {e}")
        return None
    except Exception as e:
        logger.error(f"Error collecting from candle store: {e}")
        return None


def collect_from_ticks_json(ticks_path: str) -> Optional[Dict[str, Any]]:
    """
    Collect data from ticks.json file.
    
    Args:
        ticks_path: Path to ticks.json file
        
    Returns:
        Dictionary with OHLCV data
    """
    if not os.path.exists(ticks_path):
        return None
    
    try:
        with open(ticks_path, 'r') as f:
            ticks_data = json.load(f)
        
        logger.info(f"Loaded {len(ticks_data)} instruments from ticks.json")
        return ticks_data
        
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error loading ticks.json: {e}")
        return None


def collect_from_pickle(pickle_path: str) -> Optional[Dict[str, Any]]:
    """
    Collect data from pickle file.
    
    Args:
        pickle_path: Path to pickle file
        
    Returns:
        Dictionary with stock data
    """
    if not os.path.exists(pickle_path):
        return None
    
    try:
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        
        logger.info(f"Loaded {len(data)} instruments from pickle")
        return data
        
    except Exception as e:
        logger.error(f"Error loading pickle: {e}")
        return None


def publish_data(
    data: Dict[str, Any],
    output_dir: str,
    data_type: str = "candles"
) -> bool:
    """
    Publish data to the output directory structure.
    
    Args:
        data: Data dictionary to publish
        output_dir: Base output directory
        data_type: Type of data ("candles" or "ticks")
        
    Returns:
        bool: True if successful
    """
    try:
        from PKDevTools.classes import Archiver
        from datetime import datetime

        _, file_name = Archiver.afterMarketStockDataExists()
        if file_name is not None and len(file_name) > 0:
            date_part = file_name.replace(".pkl", "").replace("stock_data_", "")
            # date_part is DDMMYYYY
            dt_object = datetime.strptime(date_part, '%d%m%Y')
            today = dt_object.strftime('%Y-%m-%d')
        else:
            # fallback
            today = datetime.now().strftime("%Y-%m-%d")
            
        current_time = datetime.now().strftime("%H%M")
        
        if data_type == "candles":
            # Create timestamped snapshot directory
            snapshot_dir = os.path.join(output_dir, "candles", today)
            os.makedirs(snapshot_dir, exist_ok=True)
            
            # Save timestamped snapshot (compressed)
            snapshot_path = os.path.join(snapshot_dir, f"candles_{current_time}.json.gz")
            with gzip.open(snapshot_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f)
            logger.info(f"Saved snapshot to {snapshot_path}")
            
            # Update latest file
            latest_dir = os.path.join(output_dir, "candles")
            os.makedirs(latest_dir, exist_ok=True)
            
            latest_path = os.path.join(latest_dir, "latest.json.gz")
            with gzip.open(latest_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f)
            logger.info(f"Updated latest candles at {latest_path}")
            
            # Also save uncompressed version for easier debugging
            latest_json_path = os.path.join(output_dir, "candles_latest.json")
            with open(latest_json_path, 'w') as f:
                json.dump(data, f)
            
        elif data_type == "ticks":
            # Save ticks summary
            ticks_dir = os.path.join(output_dir, "ticks")
            os.makedirs(ticks_dir, exist_ok=True)
            
            ticks_path = os.path.join(ticks_dir, "ticks_latest.json.gz")
            with gzip.open(ticks_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f)
            logger.info(f"Saved ticks summary to {ticks_path}")
            
            # Also save uncompressed ticks.json
            ticks_json_path = os.path.join(output_dir, "ticks.json")
            with open(ticks_json_path, 'w') as f:
                json.dump(data, f)
        
        return True
        
    except Exception as e:
        logger.error(f"Error publishing {data_type} data: {e}")
        return False


def update_metadata(
    output_dir: str,
    instrument_count: int,
    candles_published: bool,
    ticks_published: bool
) -> bool:
    """
    Update metadata.json file.
    
    Args:
        output_dir: Base output directory
        instrument_count: Number of instruments in data
        candles_published: Whether candles were published
        ticks_published: Whether ticks were published
        
    Returns:
        bool: True if successful
    """
    try:
        metadata_path = os.path.join(output_dir, "metadata.json")
        
        # Load existing metadata if available
        existing_metadata = {}
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    existing_metadata = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        
        # Update metadata
        from datetime import timezone
        now = datetime.now(timezone.utc)
        metadata = {
            "last_update": now.isoformat().replace('+00:00', 'Z'),
            "last_update_timestamp": now.timestamp(),
            "instrument_count": instrument_count,
            "candles_available": candles_published,
            "ticks_available": ticks_published,
            "version": "2.0.0",
            "publisher": "publish_candle_data.py",
            "consecutive_failures": 0 if (candles_published or ticks_published) else existing_metadata.get("consecutive_failures", 0) + 1,
            "health": {
                "status": "healthy" if (candles_published or ticks_published) else "degraded",
                "last_successful_publish": now.isoformat() + "Z" if (candles_published or ticks_published) else existing_metadata.get("health", {}).get("last_successful_publish"),
            }
        }
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Updated metadata at {metadata_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating metadata: {e}")
        return False


def cleanup_old_snapshots(output_dir: str, keep_days: int = 7):
    """
    Clean up old snapshot directories.
    
    Args:
        output_dir: Base output directory
        keep_days: Number of days to keep
    """
    try:
        candles_dir = os.path.join(output_dir, "candles")
        if not os.path.exists(candles_dir):
            return
        
        cutoff_date = datetime.now() - __import__('datetime').timedelta(days=keep_days)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        
        for dirname in os.listdir(candles_dir):
            dir_path = os.path.join(candles_dir, dirname)
            
            # Skip non-date directories
            if not os.path.isdir(dir_path):
                continue
            
            try:
                # Check if directory name is a date
                if dirname < cutoff_str and len(dirname) == 10:
                    import shutil
                    shutil.rmtree(dir_path)
                    logger.info(f"Cleaned up old snapshot directory: {dirname}")
            except ValueError:
                continue
                
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Publish candle data to GitHub repository"
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Output directory for data files"
    )
    parser.add_argument(
        "--ticks-json",
        default=None,
        help="Path to ticks.json file (optional)"
    )
    parser.add_argument(
        "--pickle-path",
        default=None,
        help="Path to pickle file (optional)"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up old snapshot directories"
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=7,
        help="Number of days to keep (for cleanup)"
    )
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Collect data from available sources
    candles_data = None
    ticks_data = None
    
    # Priority 1: InMemoryCandleStore
    candles_data = collect_from_candle_store()
    
    # Priority 2: ticks.json file
    if args.ticks_json:
        ticks_data = collect_from_ticks_json(args.ticks_json)
    else:
        # Try default locations
        default_ticks_paths = [
            os.path.join(os.path.expanduser("~"), "pkscreener", "ticks.json"),
            "ticks.json",
        ]
        for path in default_ticks_paths:
            ticks_data = collect_from_ticks_json(path)
            if ticks_data:
                break
    
    # Priority 3: Pickle file
    if candles_data is None and args.pickle_path:
        candles_data = collect_from_pickle(args.pickle_path)
    
    # Publish data
    candles_published = False
    ticks_published = False
    instrument_count = 0
    
    if candles_data:
        candles_published = publish_data(candles_data, args.output_dir, "candles")
        instrument_count = len(candles_data)
    
    if ticks_data:
        ticks_published = publish_data(ticks_data, args.output_dir, "ticks")
        if instrument_count == 0:
            instrument_count = len(ticks_data)
    
    # Update metadata
    update_metadata(args.output_dir, instrument_count, candles_published, ticks_published)
    
    # Cleanup old snapshots if requested
    if args.cleanup:
        cleanup_old_snapshots(args.output_dir, args.keep_days)
    
    # Report status
    if candles_published or ticks_published:
        logger.info(f"Successfully published data for {instrument_count} instruments")
        return 0
    else:
        logger.warning("No data was published")
        return 1


if __name__ == "__main__":
    sys.exit(main())
