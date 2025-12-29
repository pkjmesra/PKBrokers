# -*- coding: utf-8 -*-
"""
End-to-end functional tests for data fallback mechanism.

Tests the complete flow of:
1. Downloading pkl files from GitHub actions-data-download branch
2. Validating pkl file contents
3. Loading data into candle store
4. Freshness validation
"""

import os
import pickle
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest


class TestDataFallbackE2E(unittest.TestCase):
    """End-to-end tests for data fallback mechanism."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up after tests."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_download_from_github_finds_pkl_file(self):
        """Test that download_from_github can find and download pkl files from GitHub."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        data_mgr = DataSharingManager(data_dir=self.temp_dir)
        
        # Try to download daily pkl - this hits the real GitHub
        success, file_path = data_mgr.download_from_github(
            file_type="daily", 
            validate_freshness=False
        )
        
        # We should either succeed or fail gracefully
        if success:
            self.assertIsNotNone(file_path)
            self.assertTrue(os.path.exists(file_path))
            
            # Verify it's a valid pkl file
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            self.assertIsInstance(data, dict)
            self.assertGreater(len(data), 0, "Pkl file should contain stock data")
            
            # Check that data has expected structure
            first_symbol = list(data.keys())[0]
            first_data = data[first_symbol]
            
            # Should be a DataFrame or dict with OHLCV data
            if hasattr(first_data, 'columns'):
                columns = list(first_data.columns)
            elif isinstance(first_data, dict) and 'columns' in first_data:
                columns = first_data['columns']
            else:
                columns = []
            
            # Verify OHLCV columns exist (case-insensitive check)
            columns_lower = [c.lower() for c in columns]
            self.assertTrue(
                any('open' in c for c in columns_lower) or 
                any('close' in c for c in columns_lower),
                f"Expected OHLCV columns, got: {columns}"
            )
            
            print(f"✅ Successfully downloaded pkl with {len(data)} instruments")
        else:
            print("⚠️ Could not download pkl from GitHub (may be network issue)")

    def test_url_patterns_cover_known_locations(self):
        """Test that URL patterns cover known pkl file locations."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager, PKSCREENER_RAW_BASE, ACTIONS_DATA_BRANCH
        
        data_mgr = DataSharingManager(data_dir=self.temp_dir)
        
        # Known locations where pkl files exist
        known_patterns = [
            "actions-data-download/stock_data_",
            "results/Data/stock_data_",
        ]
        
        # Build URLs that would be tried
        from datetime import timedelta
        today = datetime.now()
        
        urls_to_try = []
        for days_ago in range(0, 10):
            check_date = today - timedelta(days=days_ago)
            date_str_full = check_date.strftime('%d%m%Y')
            
            for pattern in known_patterns:
                url = f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/{pattern}{date_str_full}.pkl"
                urls_to_try.append(url)
        
        # Verify we have URLs for both locations
        has_actions_data = any("actions-data-download/stock_data_" in u for u in urls_to_try)
        has_results_data = any("results/Data/stock_data_" in u for u in urls_to_try)
        
        self.assertTrue(has_actions_data, "Should try actions-data-download location")
        self.assertTrue(has_results_data, "Should try results/Data location")
        
        print(f"✅ URL patterns cover {len(urls_to_try)} possible locations")

    def test_validate_pkl_freshness(self):
        """Test pkl freshness validation logic."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        import pandas as pd
        
        data_mgr = DataSharingManager(data_dir=self.temp_dir)
        
        # Create a test pkl file with known date
        test_pkl_path = os.path.join(self.temp_dir, "test_daily.pkl")
        
        # Create sample data with today's date
        today = datetime.now()
        sample_data = {
            'RELIANCE': pd.DataFrame({
                'Open': [2500.0],
                'High': [2550.0],
                'Low': [2480.0],
                'Close': [2530.0],
                'Volume': [1000000],
            }, index=[today]),
            'TCS': pd.DataFrame({
                'Open': [3500.0],
                'High': [3550.0],
                'Low': [3480.0],
                'Close': [3530.0],
                'Volume': [500000],
            }, index=[today]),
        }
        
        with open(test_pkl_path, 'wb') as f:
            pickle.dump(sample_data, f)
        
        # Validate freshness - should be fresh since it's today's data
        is_fresh, data_date, missing_days = data_mgr.validate_pkl_freshness(test_pkl_path)
        
        # Data from today should be fresh (or 0-1 days old depending on market hours)
        self.assertLessEqual(missing_days, 1, "Today's data should have 0-1 missing days")
        
        print(f"✅ Freshness validation: is_fresh={is_fresh}, date={data_date}, missing_days={missing_days}")

    def test_validate_pkl_freshness_stale_data(self):
        """Test that stale data is correctly identified."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        import pandas as pd
        
        data_mgr = DataSharingManager(data_dir=self.temp_dir)
        
        # Create a test pkl file with old date (2 weeks ago)
        test_pkl_path = os.path.join(self.temp_dir, "test_stale.pkl")
        old_date = datetime.now() - timedelta(days=14)
        
        sample_data = {
            'RELIANCE': pd.DataFrame({
                'Open': [2500.0],
                'High': [2550.0],
                'Low': [2480.0],
                'Close': [2530.0],
                'Volume': [1000000],
            }, index=[old_date]),
        }
        
        with open(test_pkl_path, 'wb') as f:
            pickle.dump(sample_data, f)
        
        # Validate freshness - should be stale
        is_fresh, data_date, missing_days = data_mgr.validate_pkl_freshness(test_pkl_path)
        
        # Data from 2 weeks ago should be stale (at least 5-10 trading days)
        self.assertFalse(is_fresh, "2-week old data should be stale")
        self.assertGreater(missing_days, 5, "Should have multiple missing trading days")
        
        print(f"✅ Stale data detected: is_fresh={is_fresh}, missing_days={missing_days}")

    def test_load_pkl_into_candle_store(self):
        """Test loading pkl data into candle store."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        import pandas as pd
        
        data_mgr = DataSharingManager(data_dir=self.temp_dir)
        candle_store = InMemoryCandleStore()
        
        # Create test pkl with sample data
        test_pkl_path = os.path.join(self.temp_dir, "test_load.pkl")
        today = datetime.now()
        
        sample_data = {
            'RELIANCE': pd.DataFrame({
                'Open': [2500.0, 2520.0, 2540.0],
                'High': [2550.0, 2570.0, 2590.0],
                'Low': [2480.0, 2500.0, 2520.0],
                'Close': [2530.0, 2550.0, 2570.0],
                'Volume': [1000000, 1100000, 1200000],
            }, index=[today - timedelta(days=2), today - timedelta(days=1), today]),
            'TCS': pd.DataFrame({
                'Open': [3500.0, 3520.0],
                'High': [3550.0, 3570.0],
                'Low': [3480.0, 3500.0],
                'Close': [3530.0, 3550.0],
                'Volume': [500000, 550000],
            }, index=[today - timedelta(days=1), today]),
        }
        
        with open(test_pkl_path, 'wb') as f:
            pickle.dump(sample_data, f)
        
        # Load into candle store
        loaded = data_mgr.load_pkl_into_candle_store(test_pkl_path, candle_store, interval='day')
        
        self.assertEqual(loaded, 2, "Should have loaded 2 instruments")
        
        # Verify instruments are registered
        self.assertGreater(len(candle_store.instruments), 0, "Candle store should have instruments")
        
        print(f"✅ Loaded {loaded} instruments into candle store")

    def test_full_fallback_flow(self):
        """Test the complete fallback flow from download to load."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        
        data_mgr = DataSharingManager(data_dir=self.temp_dir)
        candle_store = InMemoryCandleStore()
        
        # Step 1: Download from GitHub
        success, pkl_path = data_mgr.download_from_github(
            file_type="daily",
            validate_freshness=False
        )
        
        if success:
            # Step 2: Load into candle store
            loaded = data_mgr.load_pkl_into_candle_store(pkl_path, candle_store, interval='day')
            
            self.assertGreater(loaded, 0, "Should have loaded some instruments")
            
            # Step 3: Verify data is accessible
            self.assertGreater(
                len(candle_store.instruments), 0,
                "Candle store should have instruments after loading"
            )
            
            print(f"✅ Full fallback flow: Downloaded and loaded {loaded} instruments")
        else:
            print("⚠️ Could not download from GitHub (network issue), skipping full flow test")

    def test_market_hours_detection(self):
        """Test market hours detection for commit timing."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        data_mgr = DataSharingManager(data_dir=self.temp_dir)
        
        # These should not raise exceptions
        is_open = data_mgr.is_market_open()
        is_trading_day = data_mgr.is_trading_day()
        is_about_to_close = data_mgr.is_market_about_to_close()
        
        self.assertIsInstance(is_open, bool)
        self.assertIsInstance(is_trading_day, bool)
        self.assertIsInstance(is_about_to_close, bool)
        
        print(f"✅ Market status: open={is_open}, trading_day={is_trading_day}, closing_soon={is_about_to_close}")


class TestTriggerHistoryDownload(unittest.TestCase):
    """Tests for history download workflow trigger."""

    def test_trigger_without_token_fails_gracefully(self):
        """Test that trigger fails gracefully without GitHub token."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        # Ensure no token is set
        old_token = os.environ.pop('GITHUB_TOKEN', None)
        old_ci_pat = os.environ.pop('CI_PAT', None)
        
        try:
            data_mgr = DataSharingManager()
            result = data_mgr.trigger_history_download_workflow(past_offset=1)
            
            self.assertFalse(result, "Should return False without token")
            print("✅ Trigger correctly fails without GitHub token")
        finally:
            if old_token:
                os.environ['GITHUB_TOKEN'] = old_token
            if old_ci_pat:
                os.environ['CI_PAT'] = old_ci_pat


if __name__ == '__main__':
    pytest.main([__file__, '-v'])





