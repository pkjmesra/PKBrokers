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

Tick Processor
==============

This module provides a high-performance tick processor that bridges real-time
tick data from Zerodha WebSocket to the InMemoryCandleStore.

Features:
    - Receives ticks from WebSocket queue
    - Updates InMemoryCandleStore in real-time
    - Maintains instrument symbol mappings
    - Provides data access API for scans
    - Exports data to pickle/JSON formats

Example:
    >>> from pkbrokers.kite.tickProcessor import TickProcessor
    >>> 
    >>> processor = TickProcessor()
    >>> processor.start()
    >>> 
    >>> # Access candle data
    >>> df = processor.get_candles_df("RELIANCE", "5m", count=50)
"""

import json
import os
import pickle
import queue
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pytz

from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities

from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore, get_candle_store

# Constants
KOLKATA_TZ = pytz.timezone("Asia/Kolkata")
DEFAULT_PATH = Archiver.get_user_data_dir()


class TickProcessor:
    """
    High-performance tick processor that bridges WebSocket ticks to candle storage.
    
    This class:
    1. Receives tick data from a queue (from WebSocket client)
    2. Updates the InMemoryCandleStore with each tick
    3. Maintains instrument symbol mappings
    4. Provides convenient data access methods
    
    Attributes:
        candle_store: Reference to InMemoryCandleStore
        instruments_map: Mapping of instrument_token to trading_symbol
        logger: Logger instance
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(
        self,
        data_queue: queue.Queue = None,
        auto_start: bool = False,
    ):
        """
        Initialize the tick processor.
        
        Args:
            data_queue: Queue to receive tick data from (optional)
            auto_start: Whether to start processing immediately
        """
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._initialized = True
        self.candle_store = get_candle_store()
        self.data_queue = data_queue
        self.logger = default_logger()
        self.instruments_map: Dict[int, str] = {}
        self._stop_event = threading.Event()
        self._processor_thread = None
        
        # Statistics
        self.stats = {
            'ticks_received': 0,
            'ticks_processed': 0,
            'errors': 0,
            'start_time': None,
        }
        
        # Load instrument mappings
        self._load_instruments()
        
        if auto_start and data_queue is not None:
            self.start()
    
    def _load_instruments(self):
        """Load instrument token to symbol mappings."""
        try:
            from pkbrokers.kite.instruments import KiteInstruments
            from PKDevTools.classes.Environment import PKEnvironment
            
            kite = KiteInstruments(
                api_key="kitefront",
                access_token=PKEnvironment().KTOKEN,
            )
            
            # Get all equities with token and symbol
            equities = kite.get_equities(column_names="instrument_token,tradingsymbol")
            
            if equities is not None and len(equities) > 0:
                for _, row in equities.iterrows():
                    token = row.get('instrument_token')
                    symbol = row.get('tradingsymbol')
                    if token and symbol:
                        self.instruments_map[int(token)] = symbol
                        self.candle_store.register_instrument(int(token), symbol)
                
                self.logger.info(f"Loaded {len(self.instruments_map)} instrument mappings")
        except Exception as e:
            self.logger.warning(f"Could not load instrument mappings: {e}")
    
    def set_data_queue(self, data_queue: queue.Queue):
        """Set the data queue for receiving ticks."""
        self.data_queue = data_queue
    
    def start(self):
        """Start the tick processing thread."""
        if self._processor_thread is not None and self._processor_thread.is_alive():
            self.logger.warning("Tick processor already running")
            return
        
        self._stop_event.clear()
        self.stats['start_time'] = time.time()
        
        self._processor_thread = threading.Thread(
            target=self._process_loop,
            daemon=True,
            name="tick_processor"
        )
        self._processor_thread.start()
        self.logger.info("Tick processor started")
    
    def stop(self):
        """Stop the tick processing thread."""
        self._stop_event.set()
        if self._processor_thread is not None:
            self._processor_thread.join(timeout=5)
        self.logger.info("Tick processor stopped")
    
    def _process_loop(self):
        """Main processing loop."""
        while not self._stop_event.is_set():
            try:
                if self.data_queue is None:
                    time.sleep(0.1)
                    continue
                
                try:
                    tick_data = self.data_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                
                self.stats['ticks_received'] += 1
                self.process_tick(tick_data)
                
            except Exception as e:
                self.logger.error(f"Error in tick processor: {e}")
                self.stats['errors'] += 1
                time.sleep(0.01)
    
    def process_tick(self, tick_data: Dict[str, Any]) -> bool:
        """
        Process a single tick.
        
        Args:
            tick_data: Tick data dictionary from WebSocket
            
        Returns:
            bool: True if processed successfully
        """
        try:
            if tick_data is None or tick_data.get('type') != 'tick':
                return False
            
            instrument_token = tick_data.get('instrument_token')
            
            # Add trading symbol if not present
            if 'trading_symbol' not in tick_data and instrument_token in self.instruments_map:
                tick_data['trading_symbol'] = self.instruments_map[instrument_token]
            
            # Process through candle store
            success = self.candle_store.process_tick(tick_data)
            
            if success:
                self.stats['ticks_processed'] += 1
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing tick: {e}")
            self.stats['errors'] += 1
            return False
    
    def process_ticks_batch(self, ticks: List[Dict[str, Any]]) -> int:
        """Process multiple ticks."""
        processed = 0
        for tick in ticks:
            if self.process_tick(tick):
                processed += 1
        return processed
    
    # =====================
    # Data Access Methods
    # =====================
    
    def get_candles(
        self,
        symbol: str = None,
        instrument_token: int = None,
        interval: str = '5m',
        count: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get candles for an instrument.
        
        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            instrument_token: Instrument token (alternative to symbol)
            interval: Candle interval ('1m', '2m', '3m', '4m', '5m', '10m', '15m', '30m', '60m', 'day')
            count: Number of candles to return
            
        Returns:
            List of candle dictionaries
        """
        return self.candle_store.get_candles(
            instrument_token=instrument_token,
            trading_symbol=symbol,
            interval=interval,
            count=count,
        )
    
    def get_candles_df(
        self,
        symbol: str = None,
        instrument_token: int = None,
        interval: str = '5m',
        count: int = 50,
    ) -> pd.DataFrame:
        """
        Get candles as a pandas DataFrame.
        
        Args:
            symbol: Trading symbol
            instrument_token: Instrument token
            interval: Candle interval
            count: Number of candles
            
        Returns:
            DataFrame with OHLCV columns
        """
        return self.candle_store.get_ohlcv_dataframe(
            instrument_token=instrument_token,
            trading_symbol=symbol,
            interval=interval,
            count=count,
        )
    
    def get_latest_price(self, symbol: str = None, instrument_token: int = None) -> Optional[float]:
        """Get the latest price for an instrument."""
        return self.candle_store.get_latest_price(
            instrument_token=instrument_token,
            trading_symbol=symbol,
        )
    
    def get_day_ohlcv(self, symbol: str = None, instrument_token: int = None) -> Optional[Dict[str, Any]]:
        """Get today's OHLCV data for an instrument."""
        return self.candle_store.get_day_ohlcv(
            instrument_token=instrument_token,
            trading_symbol=symbol,
        )
    
    def get_all_day_ohlcv(self) -> Dict[str, Dict[str, Any]]:
        """
        Get today's OHLCV for all instruments with their symbols.
        
        Returns:
            Dictionary with trading_symbol as key
        """
        result = {}
        all_ohlcv = self.candle_store.get_all_instruments_ohlcv(interval='day')
        
        for token, data in all_ohlcv.items():
            symbol = data.get('trading_symbol') or self.instruments_map.get(token, str(token))
            result[symbol] = data
        
        return result
    
    def get_multi_timeframe_candles(
        self,
        symbol: str = None,
        instrument_token: int = None,
        intervals: List[str] = None,
        count: int = 50,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get candles for multiple timeframes at once.
        
        Args:
            symbol: Trading symbol
            instrument_token: Instrument token
            intervals: List of intervals (default: all)
            count: Number of candles per interval
            
        Returns:
            Dictionary mapping interval to candle list
        """
        if intervals is None:
            intervals = ['1m', '2m', '3m', '4m', '5m', '10m', '15m', '30m', '60m', 'day']
        
        result = {}
        for interval in intervals:
            result[interval] = self.get_candles(
                symbol=symbol,
                instrument_token=instrument_token,
                interval=interval,
                count=count,
            )
        
        return result
    
    # =====================
    # Export Methods
    # =====================
    
    def export_pickle(self, file_path: str = None) -> bool:
        """
        Export data to pickle file in InstrumentDataManager format.
        
        Args:
            file_path: Output file path (defaults to standard location)
            
        Returns:
            bool: True if export successful
        """
        if file_path is None:
            exists, path = Archiver.afterMarketStockDataExists(date_suffix=True)
            file_path = os.path.join(DEFAULT_PATH, path)
        
        try:
            data = self.candle_store.export_to_pickle_format()
            
            with open(file_path, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            self.logger.info(f"Exported {len(data)} instruments to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting pickle: {e}")
            return False
    
    def export_ticks_json(self, file_path: str = None) -> bool:
        """
        Export data to ticks.json format.
        
        Args:
            file_path: Output file path
            
        Returns:
            bool: True if export successful
        """
        if file_path is None:
            file_path = os.path.join(DEFAULT_PATH, "ticks.json")
        
        try:
            data = self.candle_store.export_to_ticks_json()
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.info(f"Exported {len(data)} instruments to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting ticks.json: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processor statistics."""
        store_stats = self.candle_store.get_stats()
        
        uptime = 0
        if self.stats['start_time']:
            uptime = time.time() - self.stats['start_time']
        
        return {
            'processor': {
                **self.stats,
                'uptime_seconds': uptime,
                'instruments_mapped': len(self.instruments_map),
            },
            'store': store_stats,
        }


# Singleton accessor
def get_tick_processor() -> TickProcessor:
    """Get the global tick processor instance."""
    return TickProcessor()


class HighPerformanceDataProvider:
    """
    Convenience class for accessing candle data from PKScreener scans.
    
    This class provides a simple interface for scan routines to access
    real-time candle data without database dependency.
    
    Example:
        >>> from pkbrokers.kite.tickProcessor import HighPerformanceDataProvider
        >>> 
        >>> provider = HighPerformanceDataProvider()
        >>> 
        >>> # Get 5-minute candles for RELIANCE
        >>> df = provider.get_stock_data("RELIANCE", interval="5m")
        >>> 
        >>> # Get daily data for multiple stocks
        >>> data = provider.get_stocks_data(["RELIANCE", "TCS", "INFY"])
    """
    
    def __init__(self):
        """Initialize the data provider."""
        self.processor = get_tick_processor()
        self.candle_store = get_candle_store()
        self.logger = default_logger()
    
    def get_stock_data(
        self,
        symbol: str,
        interval: str = 'day',
        count: int = 100,
    ) -> pd.DataFrame:
        """
        Get OHLCV data for a stock.
        
        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            interval: Candle interval
            count: Number of candles
            
        Returns:
            DataFrame with OHLCV data
        """
        return self.processor.get_candles_df(
            symbol=symbol,
            interval=interval,
            count=count,
        )
    
    def get_stocks_data(
        self,
        symbols: List[str],
        interval: str = 'day',
        count: int = 100,
    ) -> Dict[str, pd.DataFrame]:
        """
        Get OHLCV data for multiple stocks.
        
        Args:
            symbols: List of trading symbols
            interval: Candle interval
            count: Number of candles per stock
            
        Returns:
            Dictionary mapping symbol to DataFrame
        """
        result = {}
        for symbol in symbols:
            df = self.get_stock_data(symbol, interval, count)
            if not df.empty:
                result[symbol] = df
        return result
    
    def get_intraday_data(
        self,
        symbol: str,
        interval: str = '5m',
    ) -> pd.DataFrame:
        """
        Get intraday candle data for a stock.
        
        Args:
            symbol: Trading symbol
            interval: Intraday interval (1m, 2m, 3m, 4m, 5m, 10m, 15m, 30m, 60m)
            
        Returns:
            DataFrame with today's intraday candles
        """
        # Get full day's worth of candles
        max_candles = {
            '1m': 390,
            '2m': 195,
            '3m': 130,
            '4m': 98,
            '5m': 78,
            '10m': 39,
            '15m': 26,
            '30m': 13,
            '60m': 7,
        }
        count = max_candles.get(interval, 100)
        
        return self.get_stock_data(symbol, interval, count)
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get the current price for a stock."""
        return self.processor.get_latest_price(symbol=symbol)
    
    def get_current_ohlcv(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current day's OHLCV for a stock."""
        return self.processor.get_day_ohlcv(symbol=symbol)
    
    def get_market_data(self) -> Dict[str, Dict[str, Any]]:
        """
        Get current day's OHLCV for all stocks.
        
        Returns:
            Dictionary mapping symbol to OHLCV data
        """
        return self.processor.get_all_day_ohlcv()
    
    def is_data_available(self, symbol: str) -> bool:
        """Check if data is available for a symbol."""
        token = self.candle_store.symbol_to_token.get(symbol)
        return token is not None and token in self.candle_store.instruments
    
    def get_available_symbols(self) -> List[str]:
        """Get list of all available symbols."""
        return list(self.candle_store.symbol_to_token.keys())
