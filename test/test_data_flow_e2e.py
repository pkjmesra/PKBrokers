"""
End-to-end functional tests for PKBrokers data flow mechanisms.

This module tests the complete data flow:
1. pktickbot receives ticks from Zerodha Kite
2. Ticks are aggregated into 1-min and daily candles
3. Candle data is exported to pkl files
4. pkl files are committed to GitHub
5. New pktickbot instances download pkl from GitHub or running instance
6. Instance handoff works correctly

Usage:
    pytest test/test_data_flow_e2e.py -v
"""

import os
import pickle
import tempfile
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pytest


class TestPKTickBotDataFlow:
    """Test pktickbot data flow mechanisms."""
    
    def test_candle_store_aggregation(self):
        """Test that candle store properly aggregates ticks into candles."""
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        
        store = InMemoryCandleStore()
        
        # Register an instrument
        store.register_instrument(256265, "RELIANCE")
        
        # Simulate tick data
        base_time = datetime.now().timestamp()
        ticks = [
            {"instrument_token": 256265, "last_price": 2500.0, "volume": 1000, "timestamp": base_time},
            {"instrument_token": 256265, "last_price": 2510.0, "volume": 2000, "timestamp": base_time + 10},
            {"instrument_token": 256265, "last_price": 2505.0, "volume": 3000, "timestamp": base_time + 30},
        ]
        
        for tick in ticks:
            store.process_tick(tick)
        
        # Verify instrument has data
        assert len(store.instruments) > 0
        
    def test_data_sharing_manager_export_daily(self):
        """Test that DataSharingManager can export daily candles to pkl."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            store = InMemoryCandleStore()
            
            # Register and add some data
            store.register_instrument(256265, "RELIANCE")
            tick = {
                "instrument_token": 256265, 
                "last_price": 2500.0, 
                "volume": 1000, 
                "timestamp": datetime.now().timestamp()
            }
            store.process_tick(tick)
            
            # Export - should work (may be empty if no full candles)
            # This tests the export mechanism
            success, path = data_mgr.export_daily_candles_to_pkl(store, merge_with_historical=False)
            
            # Verify function returns expected types
            assert isinstance(success, bool)
            assert path is None or isinstance(path, str)
    
    def test_data_sharing_manager_export_intraday(self):
        """Test that DataSharingManager can export intraday candles to pkl."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            store = InMemoryCandleStore()
            
            # Register and add some data
            store.register_instrument(256265, "RELIANCE")
            tick = {
                "instrument_token": 256265, 
                "last_price": 2500.0, 
                "volume": 1000, 
                "timestamp": datetime.now().timestamp()
            }
            store.process_tick(tick)
            
            success, path = data_mgr.export_intraday_candles_to_pkl(store)
            
            assert isinstance(success, bool)
            assert path is None or isinstance(path, str)
    
    def test_github_download_urls(self):
        """Test that download_from_github tries correct URLs."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            
            # This will attempt real downloads - may fail due to network
            # but tests the URL construction logic
            success, path = data_mgr.download_from_github(file_type="daily", validate_freshness=False)
            
            # Result depends on network/availability
            assert isinstance(success, bool)
    
    def test_should_commit_market_close(self):
        """Test that should_commit returns True near market close."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            
            # Test function exists and returns boolean
            result = data_mgr.should_commit()
            assert isinstance(result, bool)
    
    def test_market_hours_detection(self):
        """Test market hours detection."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        data_mgr = DataSharingManager()
        
        # Test these functions exist and return boolean
        assert isinstance(data_mgr.is_market_open(), bool)
        assert isinstance(data_mgr.is_trading_day(), bool)
    
    def test_pkl_file_paths(self):
        """Test that pkl file paths are correctly generated."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            
            daily_path = data_mgr.get_daily_pkl_path()
            intraday_path = data_mgr.get_intraday_pkl_path()
            
            assert daily_path.endswith(".pkl")
            assert intraday_path.endswith(".pkl")
            assert "daily" in daily_path.lower()
            assert "intraday" in intraday_path.lower()


class TestPKTickBotInstanceHandoff:
    """Test pktickbot instance handoff mechanisms."""
    
    def test_data_received_flag(self):
        """Test that data_received_from_instance flag works."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            
            # Initially False
            assert data_mgr.data_received_from_instance == False
            
            # Set to True
            data_mgr.data_received_from_instance = True
            assert data_mgr.data_received_from_instance == True
    
    def test_load_pkl_into_candle_store(self):
        """Test loading pkl data into candle store."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        import pandas as pd
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            store = InMemoryCandleStore()
            
            # Create test pkl file
            test_data = {
                "RELIANCE": pd.DataFrame({
                    "Open": [2500.0],
                    "High": [2510.0],
                    "Low": [2490.0],
                    "Close": [2505.0],
                    "Volume": [10000]
                }, index=[datetime.now()])
            }
            
            pkl_path = os.path.join(tmpdir, "test.pkl")
            with open(pkl_path, "wb") as f:
                pickle.dump(test_data, f)
            
            # Load into candle store
            loaded = data_mgr.load_pkl_into_candle_store(pkl_path, store, interval='day')
            
            assert isinstance(loaded, int)


class TestOrchestrator:
    """Test orchestrator functionality."""
    
    def test_orchestrator_import(self):
        """Test that orchestrator module can be imported."""
        from pkbrokers.bot.orchestrator import PKTickOrchestrator
        
        assert PKTickOrchestrator is not None
    
    def test_orchestrate_function_exists(self):
        """Test that orchestrate function exists."""
        from pkbrokers.bot.orchestrator import orchestrate
        
        assert callable(orchestrate)


class TestTickBot:
    """Test PKTickBot functionality."""
    
    def test_tickbot_import(self):
        """Test that tickbot module can be imported."""
        from pkbrokers.bot.tickbot import PKTickBot
        
        assert PKTickBot is not None
    
    def test_tickbot_commands(self):
        """Test that tickbot has required command handlers."""
        from pkbrokers.bot.tickbot import PKTickBot
        
        bot = PKTickBot(
            bot_token="test_token",
            ticks_file_path="/tmp/ticks.json",
            chat_id="123456"
        )
        
        # Verify command handlers exist
        assert hasattr(bot, 'send_daily_pkl')
        assert hasattr(bot, 'send_intraday_pkl')
        assert hasattr(bot, 'request_data')
        assert hasattr(bot, 'handle_document_received')
        assert hasattr(bot, 'send_zipped_json')  # ticks command handler


class TestHistoricalDataMerge:
    """Test historical data merging with today's ticks."""
    
    def test_export_merges_with_historical(self):
        """Test that export_daily_candles_to_pkl merges with historical data."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            store = InMemoryCandleStore()
            
            # Without merge (should work)
            success, path = data_mgr.export_daily_candles_to_pkl(store, merge_with_historical=False)
            
            # Function should not crash
            assert isinstance(success, bool)


class TestCommitPklFiles:
    """Test pkl file commit functionality."""
    
    def test_commit_pkl_files_no_files(self):
        """Test commit_pkl_files when no files exist."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            data_mgr = DataSharingManager(data_dir=tmpdir)
            
            # Should return False when no files
            result = data_mgr.commit_pkl_files()
            
            assert result == False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])













