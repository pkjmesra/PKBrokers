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

In-Memory Candle Store
======================

High-performance in-memory storage for real-time candle data aggregation.
This module provides instant O(1) access to OHLCV candles across all supported
timeframes without database dependency.

Supported Timeframes:
    - 1m (1 minute)
    - 2m (2 minutes)
    - 3m (3 minutes)
    - 4m (4 minutes)
    - 5m (5 minutes)
    - 10m (10 minutes)
    - 15m (15 minutes)
    - 30m (30 minutes)
    - 60m (60 minutes / 1 hour)
    - day (daily candles)

Features:
    - Zero database dependency for real-time access
    - Automatic tick aggregation into candles
    - Memory-efficient rolling window storage
    - Thread-safe operations
    - Automatic persistence to pickle files
    - Hot-reload capability for recovery

Example:
    >>> from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
    >>> 
    >>> store = InMemoryCandleStore()
    >>> store.process_tick(tick_data)
    >>> 
    >>> # Get 5-minute candles for RELIANCE
    >>> candles = store.get_candles(instrument_token=256265, interval='5m', count=50)
    >>> 
    >>> # Get current candle (forming)
    >>> current = store.get_current_candle(instrument_token=256265, interval='5m')
"""

import json
import os
import pickle
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pytz

from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger

# Constants
KOLKATA_TZ = pytz.timezone("Asia/Kolkata")
DEFAULT_PATH = Archiver.get_user_data_dir()
CANDLE_STORE_FILE = os.path.join(DEFAULT_PATH, "candle_store.pkl")
TICKS_JSON_FILE = os.path.join(DEFAULT_PATH, "ticks.json")

# Market hours (IST)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

# All supported intervals in seconds
SUPPORTED_INTERVALS = {
    '1m': 60,
    '2m': 120,
    '3m': 180,
    '4m': 240,
    '5m': 300,
    '10m': 600,
    '15m': 900,
    '30m': 1800,
    '60m': 3600,
    '1h': 3600,
    'day': 86400,
    '1d': 86400,
}

# Maximum candles to keep per interval (for memory management)
MAX_CANDLES = {
    '1m': 390,    # Full day of 1-min candles
    '2m': 195,    # Full day of 2-min candles
    '3m': 130,    # Full day of 3-min candles
    '4m': 98,     # Full day of 4-min candles
    '5m': 78,     # Full day of 5-min candles
    '10m': 39,    # Full day of 10-min candles
    '15m': 26,    # Full day of 15-min candles
    '30m': 13,    # Full day of 30-min candles
    '60m': 7,     # Full day of 60-min candles
    '1h': 7,      # Full day of 1-hour candles
    'day': 365,   # One year of daily candles
    '1d': 365,    # One year of daily candles
}


@dataclass
class Candle:
    """
    Represents a single OHLCV candle.
    
    Attributes:
        timestamp: Candle start time (Unix timestamp)
        open: Opening price
        high: Highest price
        low: Lowest price
        close: Closing price (last price)
        volume: Total volume
        oi: Open interest (for F&O)
        tick_count: Number of ticks in this candle
        is_complete: Whether candle is complete (closed)
    """
    timestamp: int
    open: float = 0.0
    high: float = 0.0
    low: float = float('inf')
    close: float = 0.0
    volume: int = 0
    oi: int = 0
    tick_count: int = 0
    is_complete: bool = False
    
    def update_with_tick(self, price: float, volume: int = 0, oi: int = 0):
        """Update candle with a new tick."""
        if self.tick_count == 0:
            self.open = price
            self.high = price
            self.low = price
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
        
        self.close = price
        self.volume += volume
        self.oi = oi  # OI is typically the latest value
        self.tick_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert candle to dictionary."""
        return {
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low if self.low != float('inf') else 0,
            'close': self.close,
            'volume': self.volume,
            'oi': self.oi,
            'tick_count': self.tick_count,
            'is_complete': self.is_complete,
        }
    
    def to_list(self) -> List:
        """Convert to list for DataFrame creation [open, high, low, close, volume]."""
        return [self.open, self.high, self.low if self.low != float('inf') else self.open, self.close, self.volume]
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Candle':
        """Create candle from dictionary."""
        return cls(
            timestamp=data.get('timestamp', 0),
            open=data.get('open', 0.0),
            high=data.get('high', 0.0),
            low=data.get('low', float('inf')),
            close=data.get('close', 0.0),
            volume=data.get('volume', 0),
            oi=data.get('oi', 0),
            tick_count=data.get('tick_count', 0),
            is_complete=data.get('is_complete', False),
        )


@dataclass
class InstrumentCandles:
    """
    Stores candles for all intervals for a single instrument.
    Uses deques for efficient append/pop operations with fixed size.
    """
    instrument_token: int
    trading_symbol: str = ""
    candles: Dict[str, Deque[Candle]] = field(default_factory=dict)
    current_candle: Dict[str, Optional[Candle]] = field(default_factory=dict)
    last_update: float = 0.0
    
    def __post_init__(self):
        """Initialize candle deques for all intervals."""
        for interval, max_size in MAX_CANDLES.items():
            if interval not in self.candles:
                self.candles[interval] = deque(maxlen=max_size)
            if interval not in self.current_candle:
                self.current_candle[interval] = None


class InMemoryCandleStore:
    """
    High-performance in-memory storage for real-time candle data.
    
    This class maintains OHLCV candles for all instruments across all supported
    timeframes. It receives tick data and automatically aggregates it into
    candles in real-time.
    
    Features:
        - O(1) access to candles for any instrument/interval
        - Thread-safe operations
        - Automatic candle completion on interval boundaries
        - Memory-efficient rolling window storage
        - Automatic persistence for disaster recovery
    
    Attributes:
        instruments: Dictionary mapping instrument_token to InstrumentCandles
        instrument_symbols: Dictionary mapping instrument_token to trading_symbol
        logger: Logger instance
        lock: Threading lock for thread safety
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern for global access."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(
        self,
        auto_persist: bool = True,
        persist_interval: int = 300,  # 5 minutes
        load_existing: bool = True,
    ):
        """
        Initialize the in-memory candle store.
        
        Args:
            auto_persist: Whether to automatically persist data to disk
            persist_interval: Interval between persists in seconds
            load_existing: Whether to load existing data from disk on startup
        """
        # Prevent re-initialization in singleton
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._initialized = True
        self.instruments: Dict[int, InstrumentCandles] = {}
        self.instrument_symbols: Dict[int, str] = {}
        self.symbol_to_token: Dict[str, int] = {}
        self.logger = default_logger()
        self.lock = threading.RLock()
        
        self.auto_persist = auto_persist
        self.persist_interval = persist_interval
        self.last_persist_time = time.time()
        
        # Statistics
        self.stats = {
            'ticks_processed': 0,
            'candles_created': 0,
            'candles_completed': 0,
            'last_tick_time': 0,
            'start_time': time.time(),
        }
        
        # Load existing data if available
        if load_existing:
            self._load_from_disk()
        
        # Start persistence thread if enabled
        if auto_persist:
            self._start_persist_thread()
    
    def _get_candle_start_time(self, timestamp: float, interval: str) -> int:
        """
        Calculate the candle start time for a given timestamp and interval.
        
        Args:
            timestamp: Unix timestamp
            interval: Interval string (e.g., '5m', '15m', 'day')
            
        Returns:
            Unix timestamp of candle start
        """
        interval_seconds = SUPPORTED_INTERVALS.get(interval, 60)
        
        if interval in ('day', '1d'):
            # Daily candles start at market open
            dt = datetime.fromtimestamp(timestamp, tz=KOLKATA_TZ)
            market_open = dt.replace(
                hour=MARKET_OPEN_HOUR,
                minute=MARKET_OPEN_MINUTE,
                second=0,
                microsecond=0
            )
            if dt < market_open:
                market_open = market_open - timedelta(days=1)
            return int(market_open.timestamp())
        else:
            # Align to interval boundaries
            return int(timestamp // interval_seconds) * interval_seconds
    
    def _get_or_create_instrument(self, instrument_token: int, trading_symbol: str = "") -> InstrumentCandles:
        """Get or create instrument candle storage."""
        if instrument_token not in self.instruments:
            self.instruments[instrument_token] = InstrumentCandles(
                instrument_token=instrument_token,
                trading_symbol=trading_symbol,
            )
            if trading_symbol:
                self.instrument_symbols[instrument_token] = trading_symbol
                self.symbol_to_token[trading_symbol] = instrument_token
        
        return self.instruments[instrument_token]
    
    def process_tick(self, tick_data: Dict[str, Any]) -> bool:
        """
        Process a single tick and update all candles.
        
        Args:
            tick_data: Dictionary with tick data:
                - instrument_token: int
                - last_price: float
                - day_volume: int (optional)
                - oi: int (optional)
                - exchange_timestamp: int/float (Unix timestamp)
                - trading_symbol: str (optional)
                
        Returns:
            bool: True if tick was processed successfully
        """
        try:
            instrument_token = tick_data.get('instrument_token')
            price = tick_data.get('last_price', 0)
            volume = tick_data.get('day_volume', 0)
            oi = tick_data.get('oi', 0)
            timestamp = tick_data.get('exchange_timestamp')
            trading_symbol = tick_data.get('trading_symbol', '')
            
            if instrument_token is None or price is None or price <= 0:
                return False
            
            # Handle timestamp
            if timestamp is None:
                timestamp = time.time()
            elif hasattr(timestamp, 'timestamp'):
                timestamp = timestamp.timestamp()
            
            with self.lock:
                instrument = self._get_or_create_instrument(instrument_token, trading_symbol)
                instrument.last_update = time.time()
                
                # Update candles for all intervals
                for interval in SUPPORTED_INTERVALS.keys():
                    self._update_candle(instrument, interval, timestamp, price, volume, oi)
                
                self.stats['ticks_processed'] += 1
                self.stats['last_tick_time'] = timestamp
            
            # Auto-persist check
            if self.auto_persist and (time.time() - self.last_persist_time) > self.persist_interval:
                self._persist_to_disk()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing tick: {e}")
            return False
    
    def process_ticks_batch(self, ticks: List[Dict[str, Any]]) -> int:
        """
        Process multiple ticks efficiently.
        
        Args:
            ticks: List of tick data dictionaries
            
        Returns:
            int: Number of ticks successfully processed
        """
        processed = 0
        for tick in ticks:
            if self.process_tick(tick):
                processed += 1
        return processed
    
    def _update_candle(
        self,
        instrument: InstrumentCandles,
        interval: str,
        timestamp: float,
        price: float,
        volume: int,
        oi: int,
    ):
        """Update or create candle for a specific interval."""
        candle_start = self._get_candle_start_time(timestamp, interval)
        current = instrument.current_candle.get(interval)
        
        if current is None or current.timestamp != candle_start:
            # New candle period
            if current is not None:
                # Complete the previous candle
                current.is_complete = True
                instrument.candles[interval].append(current)
                self.stats['candles_completed'] += 1
            
            # Start new candle
            current = Candle(timestamp=candle_start)
            instrument.current_candle[interval] = current
            self.stats['candles_created'] += 1
        
        # Update current candle with tick
        current.update_with_tick(price, volume, oi)
    
    def get_candles(
        self,
        instrument_token: int = None,
        trading_symbol: str = None,
        interval: str = '5m',
        count: int = 50,
        include_current: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get candles for an instrument and interval.
        
        Args:
            instrument_token: Instrument token (required if trading_symbol not provided)
            trading_symbol: Trading symbol (alternative to instrument_token)
            interval: Candle interval (default '5m')
            count: Number of candles to return
            include_current: Whether to include the current (forming) candle
            
        Returns:
            List of candle dictionaries, oldest first
        """
        # Resolve instrument token
        if instrument_token is None and trading_symbol:
            instrument_token = self.symbol_to_token.get(trading_symbol)
        
        if instrument_token is None:
            return []
        
        # Normalize interval
        if interval == '1h':
            interval = '60m'
        elif interval == '1d':
            interval = 'day'
        
        with self.lock:
            instrument = self.instruments.get(instrument_token)
            if instrument is None:
                return []
            
            candle_deque = instrument.candles.get(interval, deque())
            result = [c.to_dict() for c in list(candle_deque)[-count:]]
            
            if include_current:
                current = instrument.current_candle.get(interval)
                if current is not None and current.tick_count > 0:
                    result.append(current.to_dict())
            
            return result
    
    def get_current_candle(
        self,
        instrument_token: int = None,
        trading_symbol: str = None,
        interval: str = '5m',
    ) -> Optional[Dict[str, Any]]:
        """
        Get the current (forming) candle for an instrument.
        
        Args:
            instrument_token: Instrument token
            trading_symbol: Trading symbol (alternative)
            interval: Candle interval
            
        Returns:
            Current candle dictionary or None
        """
        if instrument_token is None and trading_symbol:
            instrument_token = self.symbol_to_token.get(trading_symbol)
        
        if instrument_token is None:
            return None
        
        with self.lock:
            instrument = self.instruments.get(instrument_token)
            if instrument is None:
                return None
            
            current = instrument.current_candle.get(interval)
            if current and current.tick_count > 0:
                return current.to_dict()
            
            return None
    
    def get_ohlcv_dataframe(
        self,
        instrument_token: int = None,
        trading_symbol: str = None,
        interval: str = '5m',
        count: int = 50,
    ) -> pd.DataFrame:
        """
        Get candles as a pandas DataFrame.
        
        Args:
            instrument_token: Instrument token
            trading_symbol: Trading symbol (alternative)
            interval: Candle interval
            count: Number of candles
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        candles = self.get_candles(instrument_token, trading_symbol, interval, count)
        
        if not candles:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        df = pd.DataFrame(candles)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert(KOLKATA_TZ)
        df = df.set_index('datetime')
        
        return df[['open', 'high', 'low', 'close', 'volume']]
    
    def get_latest_price(self, instrument_token: int = None, trading_symbol: str = None) -> Optional[float]:
        """Get the latest price for an instrument."""
        if instrument_token is None and trading_symbol:
            instrument_token = self.symbol_to_token.get(trading_symbol)
        
        if instrument_token is None:
            return None
        
        with self.lock:
            instrument = self.instruments.get(instrument_token)
            if instrument is None:
                return None
            
            # Get from smallest interval for most recent data
            current = instrument.current_candle.get('1m')
            if current and current.tick_count > 0:
                return current.close
            
            return None
    
    def get_day_ohlcv(self, instrument_token: int = None, trading_symbol: str = None) -> Optional[Dict[str, Any]]:
        """Get today's OHLCV data for an instrument."""
        if instrument_token is None and trading_symbol:
            instrument_token = self.symbol_to_token.get(trading_symbol)
        
        if instrument_token is None:
            return None
        
        with self.lock:
            instrument = self.instruments.get(instrument_token)
            if instrument is None:
                return None
            
            current = instrument.current_candle.get('day')
            if current and current.tick_count > 0:
                return current.to_dict()
            
            return None
    
    def get_all_instruments_ohlcv(
        self,
        interval: str = 'day',
    ) -> Dict[int, Dict[str, Any]]:
        """
        Get current OHLCV for all instruments.
        
        Args:
            interval: Candle interval (default 'day' for daily data)
            
        Returns:
            Dictionary mapping instrument_token to OHLCV data
        """
        result = {}
        
        with self.lock:
            for token, instrument in self.instruments.items():
                current = instrument.current_candle.get(interval)
                if current and current.tick_count > 0:
                    data = current.to_dict()
                    data['trading_symbol'] = self.instrument_symbols.get(token, '')
                    result[token] = data
        
        return result
    
    def export_to_pickle_format(self) -> Dict[str, Any]:
        """
        Export data in the format compatible with InstrumentDataManager pickle files.
        
        Returns:
            Dictionary in symbol-indexed DataFrame format
        """
        result = {}
        
        with self.lock:
            for token, instrument in self.instruments.items():
                symbol = self.instrument_symbols.get(token, str(token))
                
                # Get day candles
                day_candles = list(instrument.candles.get('day', []))
                current_day = instrument.current_candle.get('day')
                if current_day and current_day.tick_count > 0:
                    day_candles.append(current_day)
                
                if not day_candles:
                    continue
                
                # Convert to DataFrame-compatible format
                data = []
                index = []
                
                for candle in day_candles:
                    data.append(candle.to_list())
                    # Convert timestamp to ISO format with timezone
                    dt = datetime.fromtimestamp(candle.timestamp, tz=KOLKATA_TZ)
                    index.append(dt.isoformat())
                
                result[symbol] = {
                    'data': data,
                    'columns': ['open', 'high', 'low', 'close', 'volume'],
                    'index': index,
                }
        
        return result
    
    def export_to_ticks_json(self) -> Dict[str, Any]:
        """
        Export data in ticks.json format for PKTickBot.
        
        Returns:
            Dictionary in ticks.json format
        """
        result = {}
        
        with self.lock:
            for token, instrument in self.instruments.items():
                symbol = self.instrument_symbols.get(token, str(token))
                current = instrument.current_candle.get('day')
                
                if current and current.tick_count > 0:
                    result[str(token)] = {
                        'instrument_token': token,
                        'trading_symbol': symbol,
                        'tick_count': current.tick_count,
                        'ohlcv': {
                            'timestamp': current.timestamp,
                            'open': current.open,
                            'high': current.high,
                            'low': current.low if current.low != float('inf') else current.open,
                            'close': current.close,
                            'volume': current.volume,
                            'oi': current.oi,
                        },
                        'last_update': instrument.last_update,
                    }
        
        return result
    
    def save_ticks_json(self, file_path: str = None):
        """Save current data to ticks.json file."""
        if file_path is None:
            file_path = TICKS_JSON_FILE
        
        data = self.export_to_ticks_json()
        
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.debug(f"Saved ticks.json with {len(data)} instruments")
        except Exception as e:
            self.logger.error(f"Error saving ticks.json: {e}")
    
    def _persist_to_disk(self):
        """Persist store data to disk for recovery."""
        try:
            with self.lock:
                data = {
                    'instruments': {},
                    'instrument_symbols': self.instrument_symbols.copy(),
                    'symbol_to_token': self.symbol_to_token.copy(),
                    'stats': self.stats.copy(),
                    'saved_at': time.time(),
                }
                
                # Serialize instrument data
                for token, instrument in self.instruments.items():
                    inst_data = {
                        'instrument_token': token,
                        'trading_symbol': instrument.trading_symbol,
                        'candles': {},
                        'current_candle': {},
                        'last_update': instrument.last_update,
                    }
                    
                    # Serialize candles
                    for interval, candle_deque in instrument.candles.items():
                        inst_data['candles'][interval] = [c.to_dict() for c in candle_deque]
                    
                    # Serialize current candles
                    for interval, candle in instrument.current_candle.items():
                        if candle is not None:
                            inst_data['current_candle'][interval] = candle.to_dict()
                    
                    data['instruments'][token] = inst_data
            
            # Save to file
            with open(CANDLE_STORE_FILE, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            self.last_persist_time = time.time()
            self.logger.info(f"Persisted candle store: {len(self.instruments)} instruments")
            
            # Also save ticks.json
            self.save_ticks_json()
            
        except Exception as e:
            self.logger.error(f"Error persisting candle store: {e}")
    
    def _load_from_disk(self):
        """Load store data from disk."""
        if not os.path.exists(CANDLE_STORE_FILE):
            self.logger.info("No existing candle store file found")
            return
        
        try:
            with open(CANDLE_STORE_FILE, 'rb') as f:
                data = pickle.load(f)
            
            self.instrument_symbols = data.get('instrument_symbols', {})
            self.symbol_to_token = data.get('symbol_to_token', {})
            
            # Restore instruments
            for token, inst_data in data.get('instruments', {}).items():
                token = int(token)
                instrument = InstrumentCandles(
                    instrument_token=token,
                    trading_symbol=inst_data.get('trading_symbol', ''),
                    last_update=inst_data.get('last_update', 0),
                )
                
                # Restore candles
                for interval, candles_list in inst_data.get('candles', {}).items():
                    max_size = MAX_CANDLES.get(interval, 100)
                    instrument.candles[interval] = deque(maxlen=max_size)
                    for candle_dict in candles_list:
                        instrument.candles[interval].append(Candle.from_dict(candle_dict))
                
                # Restore current candles
                for interval, candle_dict in inst_data.get('current_candle', {}).items():
                    if candle_dict:
                        instrument.current_candle[interval] = Candle.from_dict(candle_dict)
                
                self.instruments[token] = instrument
            
            saved_at = data.get('saved_at', 0)
            age_minutes = (time.time() - saved_at) / 60
            self.logger.info(
                f"Loaded candle store: {len(self.instruments)} instruments, "
                f"data age: {age_minutes:.1f} minutes"
            )
            
        except Exception as e:
            self.logger.error(f"Error loading candle store: {e}")
    
    def _start_persist_thread(self):
        """Start background thread for periodic persistence."""
        def persist_loop():
            while True:
                time.sleep(self.persist_interval)
                try:
                    self._persist_to_disk()
                except Exception as e:
                    self.logger.error(f"Persist thread error: {e}")
        
        thread = threading.Thread(target=persist_loop, daemon=True, name="candle_persist")
        thread.start()
        self.logger.debug("Started candle persistence thread")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics."""
        with self.lock:
            return {
                **self.stats,
                'instrument_count': len(self.instruments),
                'uptime_seconds': time.time() - self.stats['start_time'],
                'memory_mb': self._estimate_memory_usage() / (1024 * 1024),
            }
    
    def _estimate_memory_usage(self) -> int:
        """Estimate memory usage in bytes."""
        # Rough estimate: ~100 bytes per candle
        total_candles = 0
        for instrument in self.instruments.values():
            for candle_deque in instrument.candles.values():
                total_candles += len(candle_deque)
            total_candles += len(instrument.current_candle)
        
        return total_candles * 100
    
    def clear(self):
        """Clear all data from the store."""
        with self.lock:
            self.instruments.clear()
            self.instrument_symbols.clear()
            self.symbol_to_token.clear()
            self.stats = {
                'ticks_processed': 0,
                'candles_created': 0,
                'candles_completed': 0,
                'last_tick_time': 0,
                'start_time': time.time(),
            }
        self.logger.info("Cleared candle store")
    
    def register_instrument(self, instrument_token: int, trading_symbol: str):
        """Register an instrument with its trading symbol."""
        with self.lock:
            self.instrument_symbols[instrument_token] = trading_symbol
            self.symbol_to_token[trading_symbol] = instrument_token
            self._get_or_create_instrument(instrument_token, trading_symbol)
    
    def get_all_symbols(self) -> List[str]:
        """
        Get all registered trading symbols.
        
        Returns:
            List of trading symbols
        """
        with self.lock:
            return list(self.symbol_to_token.keys())
    
    def get_all_instrument_tokens(self) -> List[int]:
        """
        Get all registered instrument tokens.
        
        Returns:
            List of instrument tokens
        """
        with self.lock:
            return list(self.instruments.keys())


# Singleton accessor
def get_candle_store() -> InMemoryCandleStore:
    """Get the global candle store instance."""
    return InMemoryCandleStore()
