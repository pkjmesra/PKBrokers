"""
Unit tests for the generate_pkl_from_ticks.py script.

This module tests:
1. Converting ticks.json to candle format
2. Downloading historical pkl from GitHub
3. Merging today's data with historical
4. Saving pkl files with correct format

Usage:
    pytest test/test_generate_pkl.py -v
"""

import json
import os
import pickle
import tempfile
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import pytest
import pandas as pd


class TestTicksToCandles:
    """Test converting ticks.json format to candle DataFrames."""
    
    def test_convert_ticks_to_candles_basic(self):
        """Test basic ticks to candles conversion."""
        from pkbrokers.scripts.generate_pkl_from_ticks import convert_ticks_to_candles
        
        ticks_data = {
            "256265": {
                "instrument_token": 256265,
                "trading_symbol": "RELIANCE",
                "ohlcv": {
                    "open": 2500.0,
                    "high": 2520.0,
                    "low": 2490.0,
                    "close": 2510.0,
                    "volume": 10000,
                    "timestamp": "2025-12-29T15:30:00+05:30"
                }
            },
            "260105": {
                "instrument_token": 260105,
                "trading_symbol": "TCS",
                "ohlcv": {
                    "open": 3500.0,
                    "high": 3520.0,
                    "low": 3480.0,
                    "close": 3510.0,
                    "volume": 5000,
                    "timestamp": "2025-12-29T15:30:00+05:30"
                }
            }
        }
        
        candles = convert_ticks_to_candles(ticks_data, verbose=False)
        
        assert len(candles) == 2
        assert "RELIANCE" in candles
        assert "TCS" in candles
        
        # Verify DataFrame structure
        reliance_df = candles["RELIANCE"]
        assert "Open" in reliance_df.columns
        assert "High" in reliance_df.columns
        assert "Low" in reliance_df.columns
        assert "Close" in reliance_df.columns
        assert "Volume" in reliance_df.columns
        
        # Verify values
        assert reliance_df["Close"].iloc[0] == 2510.0
        assert reliance_df["Volume"].iloc[0] == 10000
    
    def test_convert_ticks_skips_invalid_data(self):
        """Test that conversion skips invalid/empty data."""
        from pkbrokers.scripts.generate_pkl_from_ticks import convert_ticks_to_candles
        
        ticks_data = {
            "256265": {
                "instrument_token": 256265,
                "trading_symbol": "RELIANCE",
                "ohlcv": {
                    "open": 2500.0,
                    "high": 2520.0,
                    "low": 2490.0,
                    "close": 2510.0,
                    "volume": 10000,
                }
            },
            "invalid1": {
                "trading_symbol": "INVALID1",
                "ohlcv": {}  # Empty OHLCV
            },
            "invalid2": {
                "trading_symbol": "INVALID2",
                "ohlcv": {
                    "close": 0  # Zero price
                }
            },
            "invalid3": {
                "trading_symbol": "INVALID3"
                # No OHLCV at all
            }
        }
        
        candles = convert_ticks_to_candles(ticks_data, verbose=False)
        
        # Only valid instrument should be converted
        assert len(candles) == 1
        assert "RELIANCE" in candles


class TestMergeCandles:
    """Test merging today's candles with historical data."""
    
    def test_merge_with_historical(self):
        """Test merging today's data with historical."""
        from pkbrokers.scripts.generate_pkl_from_ticks import merge_candles
        
        # Create historical data
        historical = {
            "RELIANCE": pd.DataFrame({
                "Open": [2400.0, 2450.0],
                "High": [2420.0, 2480.0],
                "Low": [2390.0, 2440.0],
                "Close": [2410.0, 2470.0],
                "Volume": [8000, 9000]
            }, index=pd.to_datetime(["2025-12-27", "2025-12-28"])),
            "TCS": pd.DataFrame({
                "Open": [3400.0],
                "High": [3420.0],
                "Low": [3380.0],
                "Close": [3410.0],
                "Volume": [4000]
            }, index=pd.to_datetime(["2025-12-28"]))
        }
        
        # Create today's data
        today = {
            "RELIANCE": pd.DataFrame({
                "Open": [2500.0],
                "High": [2520.0],
                "Low": [2490.0],
                "Close": [2510.0],
                "Volume": [10000]
            }, index=pd.to_datetime(["2025-12-29"])),
            "INFY": pd.DataFrame({
                "Open": [1500.0],
                "High": [1520.0],
                "Low": [1490.0],
                "Close": [1510.0],
                "Volume": [7000]
            }, index=pd.to_datetime(["2025-12-29"]))
        }
        
        merged = merge_candles(historical, today, verbose=False)
        
        # Should have all instruments
        assert len(merged) == 3  # RELIANCE, TCS, INFY
        
        # RELIANCE should have 3 rows (2 historical + 1 today)
        assert len(merged["RELIANCE"]) == 3
        
        # TCS should have 1 row (only historical)
        assert len(merged["TCS"]) == 1
        
        # INFY should have 1 row (only today)
        assert len(merged["INFY"]) == 1
    
    def test_merge_with_no_historical(self):
        """Test merging when no historical data available."""
        from pkbrokers.scripts.generate_pkl_from_ticks import merge_candles
        
        today = {
            "RELIANCE": pd.DataFrame({
                "Open": [2500.0],
                "High": [2520.0],
                "Low": [2490.0],
                "Close": [2510.0],
                "Volume": [10000]
            }, index=pd.to_datetime(["2025-12-29"]))
        }
        
        merged = merge_candles(None, today, verbose=False)
        
        # Should just return today's data
        assert merged == today


class TestSavePklFiles:
    """Test saving pkl files."""
    
    def test_save_pkl_files(self):
        """Test saving pkl files to disk."""
        from pkbrokers.scripts.generate_pkl_from_ticks import save_pkl_files, save_intraday_pkl
        
        data = {
            "RELIANCE": pd.DataFrame({
                "Open": [2500.0],
                "High": [2520.0],
                "Low": [2490.0],
                "Close": [2510.0],
                "Volume": [10000]
            }, index=pd.to_datetime(["2025-12-29"]))
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test daily pkl
            daily_path, generic_path = save_pkl_files(data, tmpdir, verbose=False)
            
            assert os.path.exists(daily_path)
            assert os.path.exists(generic_path)
            
            # Verify pkl can be loaded
            with open(daily_path, 'rb') as f:
                loaded = pickle.load(f)
            
            assert "RELIANCE" in loaded
            assert loaded["RELIANCE"]["Close"].iloc[0] == 2510.0
            
            # Test intraday pkl
            intraday_path = save_intraday_pkl(data, tmpdir, verbose=False)
            
            assert os.path.exists(intraday_path)


class TestDownloadFunctions:
    """Test download functions with mocked network calls."""
    
    @patch('pkbrokers.scripts.generate_pkl_from_ticks.requests.get')
    def test_download_historical_pkl_success(self, mock_get):
        """Test successful download of historical pkl."""
        from pkbrokers.scripts.generate_pkl_from_ticks import download_historical_pkl
        
        # Create mock pkl data - must be > 1MB for the check to pass
        # Create larger dataset to exceed 1MB threshold
        mock_data = {}
        for i in range(500):
            mock_data[f"STOCK{i}"] = pd.DataFrame({
                "Open": [2400.0 + i] * 100,
                "High": [2420.0 + i] * 100,
                "Low": [2380.0 + i] * 100,
                "Close": [2410.0 + i] * 100,
                "Volume": [8000 + i] * 100
            }, index=pd.date_range("2025-01-01", periods=100, freq="D"))
        
        pkl_bytes = pickle.dumps(mock_data)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = pkl_bytes
        mock_get.return_value = mock_response
        
        result = download_historical_pkl(verbose=False)
        
        assert result is not None
        assert len(result) >= 100  # Should have multiple instruments
    
    @patch('pkbrokers.scripts.generate_pkl_from_ticks.requests.get')
    def test_download_ticks_json_success(self, mock_get):
        """Test successful download of ticks.json."""
        from pkbrokers.scripts.generate_pkl_from_ticks import download_ticks_json
        
        # Create mock ticks with > 100 instruments to pass validation
        mock_ticks = {}
        for i in range(150):
            mock_ticks[str(256265 + i)] = {
                "instrument_token": 256265 + i,
                "trading_symbol": f"STOCK{i}",
                "ohlcv": {
                    "open": 2500.0 + i,
                    "high": 2520.0 + i,
                    "low": 2490.0 + i,
                    "close": 2510.0 + i,
                    "volume": 10000 + i
                }
            }
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_ticks
        mock_get.return_value = mock_response
        
        result = download_ticks_json(verbose=False)
        
        assert result is not None
        assert len(result) >= 100


class TestDataSharingManagerPklConversion:
    """Test DataSharingManager's ticks.json to pkl conversion."""
    
    def test_convert_ticks_json_to_pkl(self):
        """Test DataSharingManager.convert_ticks_json_to_pkl()."""
        from pkbrokers.bot.dataSharingManager import DataSharingManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a ticks.json file
            ticks_data = {
                "256265": {
                    "instrument_token": 256265,
                    "trading_symbol": "RELIANCE",
                    "ohlcv": {
                        "open": 2500.0,
                        "high": 2520.0,
                        "low": 2490.0,
                        "close": 2510.0,
                        "volume": 10000,
                        "timestamp": "2025-12-29T15:30:00+05:30"
                    }
                }
            }
            
            ticks_path = os.path.join(tmpdir, "ticks.json")
            with open(ticks_path, 'w') as f:
                json.dump(ticks_data, f)
            
            # Create DataSharingManager and convert
            mgr = DataSharingManager(data_dir=tmpdir)
            success, pkl_path = mgr.convert_ticks_json_to_pkl(ticks_path)
            
            assert success is True
            assert pkl_path is not None
            assert os.path.exists(pkl_path)
            
            # Verify pkl content
            with open(pkl_path, 'rb') as f:
                loaded = pickle.load(f)
            
            assert "RELIANCE" in loaded


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
