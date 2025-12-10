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

Candle Aggregator Module
========================

This module provides utilities for aggregating raw tick data or lower timeframe
candles into higher timeframe OHLCV candles.

Supported Timeframes:
    - 1m (1 minute)
    - 2m (2 minutes)
    - 5m (5 minutes)
    - 15m (15 minutes)
    - 30m (30 minutes)
    - 1h (60 minutes / 1 hour)
    - 1d (1 day)

Example:
    >>> from pkbrokers.kite.candleAggregator import CandleAggregator
    >>> 
    >>> # Aggregate 1-minute candles to 5-minute
    >>> df_5min = CandleAggregator.aggregate_candles(df_1min, '5m')
    >>> 
    >>> # Aggregate ticks to 1-minute candles
    >>> df_1min = CandleAggregator.aggregate_ticks(ticks_df, '1m')
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import pandas as pd
import numpy as np
from PKDevTools.classes.log import default_logger


class CandleAggregator:
    """
    Utility class for aggregating candles and ticks into different timeframes.
    
    This class provides static methods for converting lower timeframe data
    into higher timeframe OHLCV candles.
    
    Attributes:
        INTERVALS: Mapping of interval strings to seconds
        INTERVAL_RULES: Pandas resample rules for each interval
    """
    
    INTERVALS = {
        '1m': 60,
        '2m': 120,
        '5m': 300,
        '10m': 600,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '60m': 3600,
        '1d': 86400,
        'day': 86400,
        'minute': 60,
        '5minute': 300,
        '10minute': 600,
        '15minute': 900,
        '30minute': 1800,
        '60minute': 3600,
    }
    
    INTERVAL_RULES = {
        '1m': '1min',
        '2m': '2min',
        '5m': '5min',
        '10m': '10min',
        '15m': '15min',
        '30m': '30min',
        '1h': '1h',
        '60m': '1h',
        '1d': '1D',
        'day': '1D',
        'minute': '1min',
        '5minute': '5min',
        '10minute': '10min',
        '15minute': '15min',
        '30minute': '30min',
        '60minute': '1h',
    }
    
    @staticmethod
    def normalize_interval(interval: str) -> str:
        """
        Normalize interval string to standard format.
        
        Args:
            interval: Interval string (e.g., '5m', '5minute', '5min')
            
        Returns:
            Normalized interval string (e.g., '5m')
        """
        interval = interval.lower().strip()
        
        # Handle various formats
        interval_map = {
            'minute': '1m',
            '1minute': '1m',
            '5minute': '5m',
            '10minute': '10m',
            '15minute': '15m',
            '30minute': '30m',
            '60minute': '1h',
            'hour': '1h',
            '1hour': '1h',
            'day': '1d',
            '1day': '1d',
        }
        
        return interval_map.get(interval, interval)
    
    @staticmethod
    def aggregate_candles(
        df: pd.DataFrame,
        target_interval: str,
        timestamp_column: str = None
    ) -> pd.DataFrame:
        """
        Aggregate lower timeframe candles to higher timeframe.
        
        Args:
            df: DataFrame with OHLCV data. Expected columns:
                - open, high, low, close, volume
                - Optional: timestamp (or use index)
            target_interval: Target timeframe (e.g., '5m', '15m', '1h', '1d')
            timestamp_column: Name of timestamp column (uses index if None)
            
        Returns:
            DataFrame with aggregated OHLCV candles at target interval
            
        Raises:
            ValueError: If required columns are missing
            
        Example:
            >>> # Aggregate 1-minute data to 5-minute
            >>> df_5min = CandleAggregator.aggregate_candles(df_1min, '5m')
        """
        if df is None or len(df) == 0:
            return pd.DataFrame()
        
        logger = default_logger()
        
        # Normalize interval
        target_interval = CandleAggregator.normalize_interval(target_interval)
        
        # Get resample rule
        rule = CandleAggregator.INTERVAL_RULES.get(target_interval)
        if rule is None:
            logger.warning(f"Unknown interval: {target_interval}, defaulting to 1D")
            rule = '1D'
        
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Ensure we have required columns (case-insensitive)
        df.columns = df.columns.str.lower()
        required_cols = ['open', 'high', 'low', 'close']
        
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Set up timestamp index
        if timestamp_column and timestamp_column in df.columns:
            df['_ts'] = pd.to_datetime(df[timestamp_column])
            df = df.set_index('_ts')
        elif not isinstance(df.index, pd.DatetimeIndex):
            # Try to convert index to datetime
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                logger.error(f"Could not convert index to datetime: {e}")
                return pd.DataFrame()
        
        # Perform aggregation
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
        }
        
        if 'volume' in df.columns:
            agg_dict['volume'] = 'sum'
        
        if 'oi' in df.columns:
            agg_dict['oi'] = 'last'
        
        result = df.resample(rule).agg(agg_dict)
        
        # Remove rows with NaN in required columns
        result = result.dropna(subset=['open', 'high', 'low', 'close'])
        
        logger.debug(f"Aggregated {len(df)} candles to {len(result)} {target_interval} candles")
        
        return result
    
    @staticmethod
    def aggregate_ticks(
        ticks_df: pd.DataFrame,
        target_interval: str = '1m',
        price_column: str = 'last_price',
        volume_column: str = 'volume',
        timestamp_column: str = 'timestamp'
    ) -> pd.DataFrame:
        """
        Aggregate raw tick data into OHLCV candles.
        
        Args:
            ticks_df: DataFrame with tick data. Expected columns:
                - last_price (or specified price_column): Trade price
                - volume (or specified volume_column): Trade volume
                - timestamp (or specified timestamp_column): Tick timestamp
            target_interval: Target timeframe (e.g., '1m', '5m', '15m')
            price_column: Name of price column
            volume_column: Name of volume column
            timestamp_column: Name of timestamp column
            
        Returns:
            DataFrame with OHLCV candles at target interval
            
        Example:
            >>> # Convert ticks to 1-minute candles
            >>> df_1min = CandleAggregator.aggregate_ticks(ticks_df, '1m')
        """
        if ticks_df is None or len(ticks_df) == 0:
            return pd.DataFrame()
        
        logger = default_logger()
        
        # Normalize interval
        target_interval = CandleAggregator.normalize_interval(target_interval)
        
        # Get resample rule
        rule = CandleAggregator.INTERVAL_RULES.get(target_interval)
        if rule is None:
            logger.warning(f"Unknown interval: {target_interval}, defaulting to 1T")
            rule = '1T'
        
        # Make a copy
        df = ticks_df.copy()
        df.columns = df.columns.str.lower()
        
        # Handle column names
        price_col = price_column.lower()
        volume_col = volume_column.lower()
        ts_col = timestamp_column.lower()
        
        if price_col not in df.columns:
            # Try common alternatives
            for alt in ['price', 'ltp', 'last_price', 'close']:
                if alt in df.columns:
                    price_col = alt
                    break
            else:
                raise ValueError(f"Could not find price column. Available: {list(df.columns)}")
        
        # Set up timestamp index
        if ts_col in df.columns:
            df['_ts'] = pd.to_datetime(df[ts_col])
            df = df.set_index('_ts')
        elif not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                logger.error(f"Could not convert index to datetime: {e}")
                return pd.DataFrame()
        
        # Sort by timestamp
        df = df.sort_index()
        
        # Build aggregation dictionary
        agg_dict = {
            price_col: ['first', 'max', 'min', 'last'],
        }
        
        if volume_col in df.columns:
            agg_dict[volume_col] = 'sum'
        
        # Aggregate
        result = df.resample(rule).agg(agg_dict)
        
        # Flatten multi-level columns
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
        
        # Rename columns to standard OHLCV
        column_map = {
            f'{price_col}_first': 'open',
            f'{price_col}_max': 'high',
            f'{price_col}_min': 'low',
            f'{price_col}_last': 'close',
            f'{volume_col}_sum': 'volume',
        }
        result = result.rename(columns=column_map)
        
        # Remove rows with NaN
        result = result.dropna(subset=['open', 'high', 'low', 'close'])
        
        logger.debug(f"Aggregated {len(ticks_df)} ticks to {len(result)} {target_interval} candles")
        
        return result
    
    @staticmethod
    def resample_to_multiple_timeframes(
        df: pd.DataFrame,
        intervals: List[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Resample candle data to multiple timeframes at once.
        
        Args:
            df: DataFrame with OHLCV data
            intervals: List of target intervals. Defaults to standard set:
                ['1m', '5m', '15m', '30m', '1h', '1d']
                
        Returns:
            Dictionary mapping interval to aggregated DataFrame
            
        Example:
            >>> results = CandleAggregator.resample_to_multiple_timeframes(df_1min)
            >>> df_5min = results['5m']
            >>> df_1hour = results['1h']
        """
        if intervals is None:
            intervals = ['1m', '5m', '15m', '30m', '1h', '1d']
        
        results = {}
        
        for interval in intervals:
            try:
                results[interval] = CandleAggregator.aggregate_candles(df, interval)
            except Exception as e:
                default_logger().error(f"Error aggregating to {interval}: {e}")
                results[interval] = pd.DataFrame()
        
        return results
    
    @staticmethod
    def get_interval_seconds(interval: str) -> int:
        """
        Get the number of seconds for a given interval.
        
        Args:
            interval: Interval string (e.g., '5m', '1h', '1d')
            
        Returns:
            Number of seconds in the interval
        """
        interval = CandleAggregator.normalize_interval(interval)
        return CandleAggregator.INTERVALS.get(interval, 86400)
    
    @staticmethod
    def validate_ohlcv(df: pd.DataFrame) -> bool:
        """
        Validate that a DataFrame has valid OHLCV data.
        
        Checks:
            - Required columns exist
            - High >= Low for all rows
            - High >= Open and High >= Close
            - Low <= Open and Low <= Close
            
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if df is None or len(df) == 0:
            return False
        
        df.columns = df.columns.str.lower()
        
        required = ['open', 'high', 'low', 'close']
        for col in required:
            if col not in df.columns:
                return False
        
        # Check OHLC relationships
        valid_high_low = (df['high'] >= df['low']).all()
        valid_high = ((df['high'] >= df['open']) & (df['high'] >= df['close'])).all()
        valid_low = ((df['low'] <= df['open']) & (df['low'] <= df['close'])).all()
        
        return valid_high_low and valid_high and valid_low
