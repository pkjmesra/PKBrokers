# -*- coding: utf-8 -*-
"""
Local SQLite Database for Candle Data Storage

This module provides a local SQLite database solution for storing OHLCV candle data,
serving as a fallback when the Turso remote database is unavailable.

Features:
    - Daily candle storage in `candles_daily_YYMMDD.db`
    - Intraday (1-min) candle storage in `candles_YYMMDD_intraday.db`
    - Syncs from Turso database when available
    - Falls back to tick data aggregation when Turso is blocked
    - Efficient batch insert/update operations
    - Compatible with PKScreener scan workflows

Usage:
    >>> from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
    >>> db = LocalCandleDatabase()
    >>> db.sync_from_turso()  # Try to sync from Turso
    >>> db.update_from_ticks(tick_data)  # Update from live ticks
    >>> daily_data = db.get_daily_candles()  # Get all daily data
"""

import os
import sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import pickle

import numpy as np
import pandas as pd
import pytz

from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger


class LocalCandleDatabase:
    """
    Local SQLite database manager for storing candle data.
    
    Provides persistent storage for daily and intraday candles with
    automatic fallback when remote databases are unavailable.
    """
    
    SUPPORTED_INTERVALS = ['1m', '2m', '3m', '4m', '5m', '10m', '15m', '30m', '60m', 'day']
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize the local candle database.
        
        Args:
            base_path: Base directory for database files. Defaults to user data dir.
        """
        self.logger = default_logger()
        self.timezone = pytz.timezone('Asia/Kolkata')
        
        # Set base path
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = Path(Archiver.get_user_data_dir())
        
        # Ensure directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Get current date for file naming
        self.current_date = datetime.now(self.timezone).date()
        self.date_suffix = self.current_date.strftime('%y%m%d')
        
        # Database file paths
        self.daily_db_path = self.base_path / f"candles_daily_{self.date_suffix}.db"
        self.intraday_db_path = self.base_path / f"candles_{self.date_suffix}_intraday.db"
        
        # Connection cache
        self._daily_conn = None
        self._intraday_conn = None
        
        # Initialize databases
        self._init_databases()
        
    def _init_databases(self):
        """Initialize both daily and intraday database schemas."""
        self._init_daily_db()
        self._init_intraday_db()
        
    def _init_daily_db(self):
        """Initialize the daily candle database schema."""
        conn = self._get_daily_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_candles (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                updated_at TEXT,
                PRIMARY KEY (symbol, date)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_daily_symbol ON daily_candles(symbol)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_candles(date)
        ''')
        
        conn.commit()
        self.logger.debug(f"Initialized daily database: {self.daily_db_path}")
        
    def _init_intraday_db(self):
        """Initialize the intraday candle database schema."""
        conn = self._get_intraday_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS intraday_candles (
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                interval TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                updated_at TEXT,
                PRIMARY KEY (symbol, timestamp, interval)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_symbol ON intraday_candles(symbol)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_timestamp ON intraday_candles(timestamp)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_interval ON intraday_candles(interval)
        ''')
        
        conn.commit()
        self.logger.debug(f"Initialized intraday database: {self.intraday_db_path}")
        
    def _get_daily_connection(self) -> sqlite3.Connection:
        """Get or create daily database connection."""
        if self._daily_conn is None:
            self._daily_conn = sqlite3.connect(str(self.daily_db_path), check_same_thread=False)
            self._daily_conn.row_factory = sqlite3.Row
        return self._daily_conn
        
    def _get_intraday_connection(self) -> sqlite3.Connection:
        """Get or create intraday database connection."""
        if self._intraday_conn is None:
            self._intraday_conn = sqlite3.connect(str(self.intraday_db_path), check_same_thread=False)
            self._intraday_conn.row_factory = sqlite3.Row
        return self._intraday_conn
        
    def close(self):
        """Close all database connections."""
        if self._daily_conn:
            self._daily_conn.close()
            self._daily_conn = None
        if self._intraday_conn:
            self._intraday_conn.close()
            self._intraday_conn = None
            
    def sync_from_turso(self, turso_conn=None) -> bool:
        """
        Sync data from Turso remote database to local SQLite.
        
        Args:
            turso_conn: Optional Turso database connection. If not provided,
                       will attempt to create one using environment variables.
                       
        Returns:
            bool: True if sync was successful, False otherwise.
        """
        try:
            if turso_conn is None:
                try:
                    import libsql
                    from PKDevTools.classes.Environment import PKEnvironment
                    
                    env = PKEnvironment()
                    db_url = env.DB_URL
                    auth_token = env.DB_AUTH_TOKEN
                    
                    if not db_url or not auth_token:
                        self.logger.warning("Turso credentials not available, skipping sync")
                        return False
                        
                    turso_conn = libsql.connect(database=db_url, auth_token=auth_token)
                except Exception as e:
                    if "BLOCKED" in str(e) or "reads are blocked" in str(e).lower():
                        self.logger.warning("Turso database blocked (quota exceeded), using local data only")
                    else:
                        self.logger.error(f"Failed to connect to Turso: {e}")
                    return False
            
            # Sync daily data
            self._sync_daily_from_turso(turso_conn)
            
            # Sync intraday data (last 5 days)
            self._sync_intraday_from_turso(turso_conn)
            
            self.logger.info("Successfully synced from Turso database")
            return True
            
        except Exception as e:
            if "BLOCKED" in str(e) or "reads are blocked" in str(e).lower():
                self.logger.warning("Turso database blocked during sync, using local data")
            else:
                self.logger.error(f"Error syncing from Turso: {e}")
            return False
            
    def _sync_daily_from_turso(self, turso_conn):
        """Sync daily candle data from Turso."""
        try:
            cursor = turso_conn.cursor()
            
            # Get last 365 days of data
            start_date = (datetime.now(self.timezone) - timedelta(days=365)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT symbol, date, open, high, low, close, volume
                FROM daily_candles
                WHERE date >= ?
                ORDER BY symbol, date
            ''', (start_date,))
            
            rows = cursor.fetchall()
            
            if rows:
                local_conn = self._get_daily_connection()
                local_cursor = local_conn.cursor()
                
                now = datetime.now(self.timezone).isoformat()
                
                local_cursor.executemany('''
                    INSERT OR REPLACE INTO daily_candles 
                    (symbol, date, open, high, low, close, volume, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], now) for r in rows])
                
                local_conn.commit()
                self.logger.info(f"Synced {len(rows)} daily candle records from Turso")
                
        except Exception as e:
            self.logger.error(f"Error syncing daily data from Turso: {e}")
            raise
            
    def _sync_intraday_from_turso(self, turso_conn):
        """Sync intraday candle data from Turso (last 5 days)."""
        try:
            cursor = turso_conn.cursor()
            
            # Get last 5 days of intraday data
            start_date = (datetime.now(self.timezone) - timedelta(days=5)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT symbol, timestamp, interval, open, high, low, close, volume
                FROM intraday_candles
                WHERE timestamp >= ?
                ORDER BY symbol, timestamp
            ''', (start_date,))
            
            rows = cursor.fetchall()
            
            if rows:
                local_conn = self._get_intraday_connection()
                local_cursor = local_conn.cursor()
                
                now = datetime.now(self.timezone).isoformat()
                
                local_cursor.executemany('''
                    INSERT OR REPLACE INTO intraday_candles 
                    (symbol, timestamp, interval, open, high, low, close, volume, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], now) for r in rows])
                
                local_conn.commit()
                self.logger.info(f"Synced {len(rows)} intraday candle records from Turso")
                
        except Exception as e:
            self.logger.error(f"Error syncing intraday data from Turso: {e}")
            raise
            
    def update_from_ticks(self, candle_store) -> bool:
        """
        Update local database from InMemoryCandleStore tick aggregation.
        
        Args:
            candle_store: InMemoryCandleStore instance with aggregated tick data.
            
        Returns:
            bool: True if update was successful, False otherwise.
        """
        try:
            if candle_store is None:
                self.logger.warning("No candle store provided")
                return False
                
            # Get all symbols from candle store
            symbols = candle_store.get_all_symbols()
            
            if not symbols:
                self.logger.warning("No symbols in candle store")
                return False
                
            daily_records = []
            intraday_records = []
            now = datetime.now(self.timezone).isoformat()
            today = datetime.now(self.timezone).strftime('%Y-%m-%d')
            
            for symbol in symbols:
                try:
                    # Get daily candle
                    daily_candle = candle_store.get_current_candle(
                        symbol=symbol, interval='day'
                    )
                    
                    if daily_candle:
                        daily_records.append((
                            symbol,
                            today,
                            daily_candle.get('open', 0),
                            daily_candle.get('high', 0),
                            daily_candle.get('low', 0),
                            daily_candle.get('close', 0),
                            daily_candle.get('volume', 0),
                            now
                        ))
                    
                    # Get 1-minute candles
                    candles_1m = candle_store.get_candles(
                        symbol=symbol, interval='1m', count=390  # Full trading day
                    )
                    
                    for candle in candles_1m:
                        timestamp = candle.get('timestamp', '')
                        if isinstance(timestamp, datetime):
                            timestamp = timestamp.isoformat()
                            
                        intraday_records.append((
                            symbol,
                            timestamp,
                            '1m',
                            candle.get('open', 0),
                            candle.get('high', 0),
                            candle.get('low', 0),
                            candle.get('close', 0),
                            candle.get('volume', 0),
                            now
                        ))
                        
                except Exception as e:
                    self.logger.debug(f"Error processing {symbol}: {e}")
                    continue
            
            # Batch insert daily records
            if daily_records:
                conn = self._get_daily_connection()
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT OR REPLACE INTO daily_candles 
                    (symbol, date, open, high, low, close, volume, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', daily_records)
                conn.commit()
                self.logger.info(f"Updated {len(daily_records)} daily candle records from ticks")
            
            # Batch insert intraday records
            if intraday_records:
                conn = self._get_intraday_connection()
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT OR REPLACE INTO intraday_candles 
                    (symbol, timestamp, interval, open, high, low, close, volume, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', intraday_records)
                conn.commit()
                self.logger.info(f"Updated {len(intraday_records)} intraday candle records from ticks")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating from ticks: {e}")
            return False
            
    def update_daily_candle(self, symbol: str, date_str: str, 
                           open_price: float, high: float, low: float, 
                           close: float, volume: int):
        """
        Update or insert a single daily candle.
        
        Args:
            symbol: Stock symbol
            date_str: Date in YYYY-MM-DD format
            open_price: Open price
            high: High price
            low: Low price
            close: Close price
            volume: Trading volume
        """
        conn = self._get_daily_connection()
        cursor = conn.cursor()
        now = datetime.now(self.timezone).isoformat()
        
        cursor.execute('''
            INSERT OR REPLACE INTO daily_candles 
            (symbol, date, open, high, low, close, volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, date_str, open_price, high, low, close, volume, now))
        
        conn.commit()
        
    def update_intraday_candle(self, symbol: str, timestamp: str, interval: str,
                               open_price: float, high: float, low: float,
                               close: float, volume: int):
        """
        Update or insert a single intraday candle.
        
        Args:
            symbol: Stock symbol
            timestamp: Timestamp in ISO format
            interval: Candle interval (e.g., '1m', '5m')
            open_price: Open price
            high: High price
            low: Low price
            close: Close price
            volume: Trading volume
        """
        conn = self._get_intraday_connection()
        cursor = conn.cursor()
        now = datetime.now(self.timezone).isoformat()
        
        cursor.execute('''
            INSERT OR REPLACE INTO intraday_candles 
            (symbol, timestamp, interval, open, high, low, close, volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, timestamp, interval, open_price, high, low, close, volume, now))
        
        conn.commit()
        
    def get_daily_candles(self, symbol: Optional[str] = None, 
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Get daily candle data from local database.
        
        Args:
            symbol: Optional specific symbol. If None, returns all symbols.
            start_date: Optional start date (YYYY-MM-DD format)
            end_date: Optional end date (YYYY-MM-DD format)
            
        Returns:
            Dict mapping symbol to DataFrame with OHLCV data.
        """
        conn = self._get_daily_connection()
        
        query = "SELECT symbol, date, open, high, low, close, volume FROM daily_candles WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
            
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
            
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
            
        query += " ORDER BY symbol, date"
        
        df = pd.read_sql_query(query, conn, params=params)
        
        if df.empty:
            return {}
            
        # Convert to symbol-indexed dict of DataFrames
        result = {}
        for sym in df['symbol'].unique():
            sym_df = df[df['symbol'] == sym].copy()
            sym_df['date'] = pd.to_datetime(sym_df['date'])
            sym_df.set_index('date', inplace=True)
            sym_df.drop('symbol', axis=1, inplace=True)
            result[sym] = sym_df
            
        return result
        
    def get_intraday_candles(self, symbol: Optional[str] = None,
                            interval: str = '1m',
                            start_time: Optional[str] = None,
                            end_time: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Get intraday candle data from local database.
        
        Args:
            symbol: Optional specific symbol. If None, returns all symbols.
            interval: Candle interval (default '1m')
            start_time: Optional start timestamp (ISO format)
            end_time: Optional end timestamp (ISO format)
            
        Returns:
            Dict mapping symbol to DataFrame with OHLCV data.
        """
        conn = self._get_intraday_connection()
        
        query = "SELECT symbol, timestamp, open, high, low, close, volume FROM intraday_candles WHERE interval = ?"
        params = [interval]
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
            
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
            
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
            
        query += " ORDER BY symbol, timestamp"
        
        df = pd.read_sql_query(query, conn, params=params)
        
        if df.empty:
            return {}
            
        # Convert to symbol-indexed dict of DataFrames
        result = {}
        for sym in df['symbol'].unique():
            sym_df = df[df['symbol'] == sym].copy()
            sym_df['timestamp'] = pd.to_datetime(sym_df['timestamp'])
            sym_df.set_index('timestamp', inplace=True)
            sym_df.drop('symbol', axis=1, inplace=True)
            result[sym] = sym_df
            
        return result
        
    def export_to_pickle(self, output_dir: Optional[str] = None) -> Tuple[str, str]:
        """
        Export database data to pickle files for PKScreener compatibility.
        
        Args:
            output_dir: Optional output directory. Defaults to base_path.
            
        Returns:
            Tuple of (daily_pickle_path, intraday_pickle_path)
        """
        output_path = Path(output_dir) if output_dir else self.base_path
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Export daily data
        daily_data = self.get_daily_candles()
        daily_pickle_path = output_path / f"stock_data_{self.date_suffix}.pkl"
        
        # Convert to PKScreener format
        pkl_data = {}
        for symbol, df in daily_data.items():
            pkl_data[symbol] = df.to_dict('split')
            pkl_data[symbol]['columns'] = ['open', 'high', 'low', 'close', 'volume']
            
        with open(daily_pickle_path, 'wb') as f:
            pickle.dump(pkl_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
        self.logger.info(f"Exported {len(pkl_data)} symbols to {daily_pickle_path}")
        
        # Export intraday data
        intraday_data = self.get_intraday_candles()
        intraday_pickle_path = output_path / f"intraday_stock_data_{self.date_suffix}.pkl"
        
        intraday_pkl_data = {}
        for symbol, df in intraday_data.items():
            intraday_pkl_data[symbol] = df.to_dict('split')
            intraday_pkl_data[symbol]['columns'] = ['open', 'high', 'low', 'close', 'volume']
            
        with open(intraday_pickle_path, 'wb') as f:
            pickle.dump(intraday_pkl_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
        self.logger.info(f"Exported {len(intraday_pkl_data)} symbols to {intraday_pickle_path}")
        
        return str(daily_pickle_path), str(intraday_pickle_path)
        
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        daily_conn = self._get_daily_connection()
        intraday_conn = self._get_intraday_connection()
        
        daily_cursor = daily_conn.cursor()
        intraday_cursor = intraday_conn.cursor()
        
        daily_cursor.execute("SELECT COUNT(DISTINCT symbol), COUNT(*) FROM daily_candles")
        daily_stats = daily_cursor.fetchone()
        
        intraday_cursor.execute("SELECT COUNT(DISTINCT symbol), COUNT(*) FROM intraday_candles")
        intraday_stats = intraday_cursor.fetchone()
        
        return {
            'daily': {
                'symbols': daily_stats[0],
                'records': daily_stats[1],
                'db_path': str(self.daily_db_path),
                'db_size_mb': os.path.getsize(self.daily_db_path) / (1024 * 1024) if self.daily_db_path.exists() else 0
            },
            'intraday': {
                'symbols': intraday_stats[0],
                'records': intraday_stats[1],
                'db_path': str(self.intraday_db_path),
                'db_size_mb': os.path.getsize(self.intraday_db_path) / (1024 * 1024) if self.intraday_db_path.exists() else 0
            }
        }


def sync_and_export():
    """
    Main function to sync from Turso (or ticks) and export to pickle.
    
    This is the entry point for the GitHub workflow.
    """
    from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore, get_candle_store
    
    db = LocalCandleDatabase()
    
    # Try to sync from Turso first
    turso_success = db.sync_from_turso()
    
    if not turso_success:
        # Fall back to tick data from InMemoryCandleStore
        print("Turso sync failed, using tick data from InMemoryCandleStore...")
        candle_store = get_candle_store()
        db.update_from_ticks(candle_store)
    
    # Export to pickle files
    daily_path, intraday_path = db.export_to_pickle()
    
    # Print stats
    stats = db.get_stats()
    print(f"\nDatabase Statistics:")
    print(f"  Daily: {stats['daily']['symbols']} symbols, {stats['daily']['records']} records")
    print(f"  Intraday: {stats['intraday']['symbols']} symbols, {stats['intraday']['records']} records")
    print(f"\nExported to:")
    print(f"  Daily: {daily_path}")
    print(f"  Intraday: {intraday_path}")
    
    db.close()
    return daily_path, intraday_path


if __name__ == "__main__":
    sync_and_export()
