# -*- coding: utf-8 -*-
"""
Unit tests for LocalCandleDatabase module.
"""

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz


class TestLocalCandleDatabase(unittest.TestCase):
    """Test cases for LocalCandleDatabase class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.timezone = pytz.timezone('Asia/Kolkata')
        
    def tearDown(self):
        """Clean up after tests."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_init_creates_databases(self):
        """Test that initialization creates both database files."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        self.assertTrue(db.daily_db_path.exists())
        self.assertTrue(db.intraday_db_path.exists())
        
        db.close()
        
    def test_daily_db_schema(self):
        """Test that daily database has correct schema."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        conn = db._get_daily_connection()
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(daily_candles)")
        columns = {row[1] for row in cursor.fetchall()}
        
        expected_columns = {'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'updated_at'}
        self.assertEqual(columns, expected_columns)
        
        db.close()
        
    def test_intraday_db_schema(self):
        """Test that intraday database has correct schema."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        conn = db._get_intraday_connection()
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(intraday_candles)")
        columns = {row[1] for row in cursor.fetchall()}
        
        expected_columns = {'symbol', 'timestamp', 'interval', 'open', 'high', 'low', 'close', 'volume', 'updated_at'}
        self.assertEqual(columns, expected_columns)
        
        db.close()
        
    def test_update_daily_candle(self):
        """Test updating a daily candle."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        db.update_daily_candle(
            symbol='RELIANCE',
            date_str='2025-12-24',
            open_price=1200.0,
            high=1250.0,
            low=1180.0,
            close=1230.0,
            volume=1000000
        )
        
        result = db.get_daily_candles(symbol='RELIANCE')
        
        self.assertIn('RELIANCE', result)
        df = result['RELIANCE']
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['close'], 1230.0)
        
        db.close()
        
    def test_update_intraday_candle(self):
        """Test updating an intraday candle."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        db.update_intraday_candle(
            symbol='RELIANCE',
            timestamp='2025-12-24T09:15:00+05:30',
            interval='1m',
            open_price=1200.0,
            high=1205.0,
            low=1198.0,
            close=1202.0,
            volume=50000
        )
        
        result = db.get_intraday_candles(symbol='RELIANCE', interval='1m')
        
        self.assertIn('RELIANCE', result)
        df = result['RELIANCE']
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['close'], 1202.0)
        
        db.close()
        
    def test_get_stats(self):
        """Test getting database statistics."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        # Add some test data
        db.update_daily_candle('RELIANCE', '2025-12-24', 100, 110, 90, 105, 1000)
        db.update_daily_candle('TCS', '2025-12-24', 3000, 3100, 2900, 3050, 500)
        
        stats = db.get_stats()
        
        self.assertEqual(stats['daily']['symbols'], 2)
        self.assertEqual(stats['daily']['records'], 2)
        
        db.close()
        
    def test_export_to_pickle(self):
        """Test exporting to pickle files."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        # Add test data
        db.update_daily_candle('RELIANCE', '2025-12-24', 100, 110, 90, 105, 1000)
        db.update_daily_candle('TCS', '2025-12-24', 3000, 3100, 2900, 3050, 500)
        
        daily_path, intraday_path = db.export_to_pickle()
        
        self.assertTrue(os.path.exists(daily_path))
        self.assertTrue(os.path.exists(intraday_path))
        
        # Verify pickle contents
        import pickle
        with open(daily_path, 'rb') as f:
            data = pickle.load(f)
        
        self.assertIn('RELIANCE', data)
        self.assertIn('TCS', data)
        
        db.close()
        
    def test_sync_from_turso_blocked(self):
        """Test handling of Turso blocked error."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        # Should return False when no connection available
        with patch.dict(os.environ, {}, clear=True):
            result = db.sync_from_turso()
            self.assertFalse(result)
        
        db.close()
        
    def test_close_connections(self):
        """Test closing database connections."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        # Access connections to create them
        _ = db._get_daily_connection()
        _ = db._get_intraday_connection()
        
        db.close()
        
        # Connections should be None after close
        self.assertIsNone(db._daily_conn)
        self.assertIsNone(db._intraday_conn)
    
    def test_update_from_ticks_with_mock_candle_store(self):
        """Test updating database from a mock candle store."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        # Create a mock candle store
        mock_store = MagicMock()
        mock_store.get_all_symbols.return_value = ['RELIANCE', 'TCS']
        mock_store.get_current_candle.return_value = {
            'open': 1200.0,
            'high': 1250.0,
            'low': 1180.0,
            'close': 1230.0,
            'volume': 1000000
        }
        mock_store.get_candles.return_value = [
            {
                'timestamp': datetime.now(self.timezone),
                'open': 1200.0,
                'high': 1205.0,
                'low': 1198.0,
                'close': 1202.0,
                'volume': 50000
            }
        ]
        
        # Update from mock store
        result = db.update_from_ticks(mock_store)
        
        self.assertTrue(result)
        
        # Verify data was inserted
        stats = db.get_stats()
        self.assertGreaterEqual(stats['daily']['symbols'], 1)
        
        db.close()
    
    def test_update_from_ticks_with_none_store(self):
        """Test update_from_ticks handles None store gracefully."""
        from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase
        
        db = LocalCandleDatabase(base_path=self.temp_dir)
        
        result = db.update_from_ticks(None)
        
        self.assertFalse(result)
        
        db.close()


class TestInMemoryCandleStore(unittest.TestCase):
    """Test cases for InMemoryCandleStore get_all_symbols method."""
    
    def test_get_all_symbols(self):
        """Test getting all registered symbols."""
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        
        # Clear any existing singleton
        InMemoryCandleStore._instance = None
        
        store = InMemoryCandleStore(auto_persist=False, load_existing=False)
        
        # Register some instruments
        store.register_instrument(256265, 'RELIANCE')
        store.register_instrument(340481, 'TCS')
        store.register_instrument(738561, 'INFY')
        
        symbols = store.get_all_symbols()
        
        self.assertIn('RELIANCE', symbols)
        self.assertIn('TCS', symbols)
        self.assertIn('INFY', symbols)
        self.assertEqual(len(symbols), 3)
        
        # Clear singleton for other tests
        InMemoryCandleStore._instance = None
    
    def test_get_all_instrument_tokens(self):
        """Test getting all registered instrument tokens."""
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        
        # Clear any existing singleton
        InMemoryCandleStore._instance = None
        
        store = InMemoryCandleStore(auto_persist=False, load_existing=False)
        
        # Register and process ticks
        store.register_instrument(256265, 'RELIANCE')
        store.process_tick({
            'instrument_token': 256265,
            'last_price': 1200.0,
            'day_volume': 1000,
            'exchange_timestamp': datetime.now().timestamp()
        })
        
        tokens = store.get_all_instrument_tokens()
        
        self.assertIn(256265, tokens)
        
        # Clear singleton for other tests
        InMemoryCandleStore._instance = None


class TestInstrumentDataManagerMarketHours(unittest.TestCase):
    """Test cases for InstrumentDataManager market hours functionality."""
    
    def test_merge_realtime_data_with_historical(self):
        """Test merging real-time data with historical data."""
        from pkbrokers.kite.datamanager import InstrumentDataManager
        
        manager = InstrumentDataManager()
        
        # Create historical data
        historical = {
            'RELIANCE': {
                'data': [[1200, 1210, 1190, 1205, 100000]],
                'columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
                'index': ['2025-12-23T09:15:00+05:30']
            }
        }
        
        # Create real-time data
        realtime = {
            'RELIANCE': {
                'data': [[1210, 1250, 1200, 1240, 200000]],
                'columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
                'index': ['2025-12-24T09:15:00+05:30']
            },
            'TCS': {
                'data': [[3000, 3100, 2900, 3050, 50000]],
                'columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
                'index': ['2025-12-24T09:15:00+05:30']
            }
        }
        
        merged = manager._merge_realtime_data_with_historical(historical, realtime)
        
        # Check RELIANCE has both historical and real-time data
        self.assertIn('RELIANCE', merged)
        self.assertEqual(len(merged['RELIANCE']['data']), 2)
        
        # Check TCS was added from real-time
        self.assertIn('TCS', merged)
        self.assertEqual(len(merged['TCS']['data']), 1)
    
    def test_merge_empty_historical(self):
        """Test merging when historical is empty."""
        from pkbrokers.kite.datamanager import InstrumentDataManager
        
        manager = InstrumentDataManager()
        
        realtime = {
            'RELIANCE': {
                'data': [[1210, 1250, 1200, 1240, 200000]],
                'columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
                'index': ['2025-12-24T09:15:00+05:30']
            }
        }
        
        merged = manager._merge_realtime_data_with_historical({}, realtime)
        
        self.assertEqual(merged, realtime)
    
    def test_merge_empty_realtime(self):
        """Test merging when real-time is empty."""
        from pkbrokers.kite.datamanager import InstrumentDataManager
        
        manager = InstrumentDataManager()
        
        historical = {
            'RELIANCE': {
                'data': [[1200, 1210, 1190, 1205, 100000]],
                'columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
                'index': ['2025-12-23T09:15:00+05:30']
            }
        }
        
        merged = manager._merge_realtime_data_with_historical(historical, {})
        
        self.assertEqual(merged, historical)


class TestTicksDownloadAndConversion(unittest.TestCase):
    """Test cases for ticks download and conversion functionality."""
    
    def test_convert_ticks_to_symbol_format(self):
        """Test converting ticks.json format to symbol-indexed format."""
        from pkbrokers.kite.datamanager import InstrumentDataManager
        
        manager = InstrumentDataManager()
        
        # Sample ticks data in the format from GitHub
        ticks_data = {
            '256265': {
                'instrument_token': 256265,
                'trading_symbol': 'RELIANCE',
                'ohlcv': {
                    'open': 1200.0,
                    'high': 1220.0,
                    'low': 1190.0,
                    'close': 1210.0,
                    'volume': 100000,
                    'timestamp': '2025-12-24T15:30:00'
                },
                'prev_day_close': 1195.0
            },
            '340481': {
                'instrument_token': 340481,
                'trading_symbol': 'TCS',
                'ohlcv': {
                    'open': 3500.0,
                    'high': 3550.0,
                    'low': 3490.0,
                    'close': 3540.0,
                    'volume': 50000,
                    'timestamp': '2025-12-24T15:30:00'
                },
                'prev_day_close': 3480.0
            }
        }
        
        result = manager._convert_ticks_to_symbol_format(ticks_data)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertIn('RELIANCE', result)
        self.assertIn('TCS', result)
        
        # Check RELIANCE data structure
        reliance_data = result['RELIANCE']
        self.assertIn('data', reliance_data)
        self.assertIn('columns', reliance_data)
        self.assertIn('index', reliance_data)
        self.assertEqual(reliance_data['columns'], ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'])
        self.assertEqual(len(reliance_data['data']), 1)
        self.assertEqual(reliance_data['data'][0][3], 1210.0)  # Close price
    
    def test_convert_ticks_with_missing_data(self):
        """Test handling of incomplete tick data."""
        from pkbrokers.kite.datamanager import InstrumentDataManager
        
        manager = InstrumentDataManager()
        
        # Ticks with missing fields
        ticks_data = {
            '256265': {
                'instrument_token': 256265,
                # Missing trading_symbol
                'ohlcv': {'open': 100, 'high': 110, 'low': 95, 'close': 105, 'volume': 1000}
            },
            '340481': {
                'instrument_token': 340481,
                'trading_symbol': 'TCS',
                # Missing ohlcv
            },
            '738561': {
                'instrument_token': 738561,
                'trading_symbol': 'INFY',
                'ohlcv': {'open': 1500, 'high': 1520, 'low': 1480, 'close': 1510, 'volume': 20000}
            }
        }
        
        result = manager._convert_ticks_to_symbol_format(ticks_data)
        
        # Should only have INFY (the complete one)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertIn('INFY', result)
    
    def test_convert_ticks_with_zero_close(self):
        """Test that ticks with zero close price are skipped."""
        from pkbrokers.kite.datamanager import InstrumentDataManager
        
        manager = InstrumentDataManager()
        
        ticks_data = {
            '256265': {
                'instrument_token': 256265,
                'trading_symbol': 'RELIANCE',
                'ohlcv': {'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0}
            }
        }
        
        result = manager._convert_ticks_to_symbol_format(ticks_data)
        
        # Should be None or empty since close is 0
        self.assertTrue(result is None or len(result) == 0)


if __name__ == '__main__':
    unittest.main()
