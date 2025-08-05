import unittest
from unittest.mock import Mock, patch, MagicMock
import asyncio
import json
import struct
import sqlite3
import os
import pytest
from datetime import datetime, timedelta
from collections import namedtuple
from PKDevTools.classes import Archiver

# Import the classes to test (adjust import path as needed)
from pkbrokers.kite.ticks import (
    TickMonitor,
    ThreadSafeDatabase,
    ZerodhaWebSocketParser,
    ZerodhaWebSocketClient,
    KiteTokenWatcher,
    Tick,
    IndexTick,
    DepthEntry,
    MarketDepth,
    DEFAULT_PATH
)

class TestTickMonitor(unittest.TestCase):
    def setUp(self):
        self.test_db = os.path.join(DEFAULT_PATH, 'test_ticks.db')
        self.monitor = TickMonitor(db_path=self.test_db)
        
    def tearDown(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
    
    @pytest.fixture
    def monitor_fixture(self):
        return TickMonitor(token_batches=[[1234, 5678]])
    
    @pytest.fixture
    def tst_db(self):
        db_path = ":memory:"
        conn = sqlite3.connect(db_path)
        yield conn
        conn.close()
    
    @pytest.mark.asyncio
    async def test_get_stale_instruments(self, monitor, tst_db):
        # Setup test data
        tst_db.executescript("""
            CREATE TABLE ticks(instrument_token INTEGER PRIMARY KEY, timestamp DATETIME);
            CREATE TABLE instrument_last_update(instrument_token INTEGER PRIMARY KEY, last_updated DATETIME);
            
            INSERT INTO ticks VALUES (1234, datetime('now'));
            INSERT INTO instrument_last_update VALUES (1234, datetime('now', '-2 minutes'));
            
            INSERT INTO ticks VALUES (5678, datetime('now'));
            INSERT INTO instrument_last_update VALUES (5678, datetime('now', '-5 minutes'));
        """)
        
        with patch('sqlite3.connect', return_value=tst_db):
            stale = await monitor._get_stale_instruments([1234, 5678], stale_minutes=3)
            assert 5678 in stale  # Should be stale
            assert 1234 not in stale  # Should be fresh

    @pytest.mark.asyncio
    async def test_monitor_stale_updates(self, monitor_fixture):
        with patch.object(monitor_fixture, '_check_stale_instruments', return_value=[1234]) as mock_check, \
             patch.object(monitor_fixture, '_handle_stale_instruments') as mock_handle:
            
            await monitor_fixture.monitor_stale_updates()
            mock_check.assert_called_once()
            mock_handle.assert_called_once_with([1234])

    @pytest.mark.asyncio
    async def test_check_stale_instruments(self, monitor_fixture):
        with patch.object(monitor_fixture, '_get_stale_instruments', side_effect=[[5678], []]) as mock_get:
            # First call with stale instruments
            stale = await monitor_fixture._check_stale_instruments([[1234, 5678], [9012]])
            assert stale == [5678]
            
            # Second call with no stale instruments
            stale = await monitor_fixture._check_stale_instruments([[1234, 5678]])
            assert stale == []

    @pytest.mark.asyncio
    async def test_handle_stale_instruments(self, monitor_fixture, caplog):
        await monitor_fixture._handle_stale_instruments([1234, 5678])
        assert "Following instruments (2) have stale updates" in caplog.text

    @patch('sqlite3.connect')
    async def test_get_stale_instruments(self, mock_connect):
        # Setup mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1234,), (5678,)]
        
        # Test with sample tokens
        tokens = [1234, 5678, 9012]
        stale = await self.monitor._get_stale_instruments(tokens)
        
        # Verify results
        self.assertEqual(len(stale), 2)
        self.assertIn(1234, stale)
        self.assertIn(5678, stale)
        
        # Verify query construction
        mock_cursor.execute.assert_called_once()
        args = mock_cursor.execute.call_args[0][1]
        self.assertEqual(args[:-1], tuple(tokens))
        self.assertEqual(args[-1], 1)  # stale_minutes

class TestThreadSafeDatabase(unittest.TestCase):
    def setUp(self):
        self.test_db = os.path.join(DEFAULT_PATH, 'test_ticks.db')
        self.db = ThreadSafeDatabase(db_path=self.test_db)
        
    def tearDown(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
    
    def test_initialize_db(self):
        # Verify tables are created
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            self.assertIn('ticks', tables)
            self.assertIn('market_depth', tables)
            self.assertIn('instrument_last_update', tables)
    
    def test_insert_ticks(self):
        test_ticks = [{
            'instrument_token': 1234,
            'timestamp': datetime.now(),
            'last_price': 100.0,
            'day_volume': 1000,
            'oi': 500,
            'buy_quantity': 600,
            'sell_quantity': 400,
            'high_price': 101.0,
            'low_price': 99.0,
            'open_price': 100.5,
            'prev_day_close': 99.5,
            'depth': {
                'bid': [{'price': 99.9, 'quantity': 100, 'orders': 5}],
                'ask': [{'price': 100.1, 'quantity': 100, 'orders': 5}]
            }
        }]
        
        # Test insertion
        self.db.insert_ticks(test_ticks)
        
        # Verify data
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM ticks")
            self.assertEqual(cursor.fetchone()[0], 1)
            
            cursor.execute("SELECT COUNT(*) FROM market_depth")
            self.assertEqual(cursor.fetchone()[0], 2)

class TestZerodhaWebSocketParser(unittest.TestCase):
    def setUp(self):
        self.parser = ZerodhaWebSocketParser
    
    def test_parse_binary_message(self):
        # Create a test binary message with 2 packets
        packet1 = struct.pack('>ii', 1234, 10000)  # token=1234, price=100.00
        packet2 = struct.pack('>ii', 5678, 20000)  # token=5678, price=200.00
        
        # Build message: 2 packets, then lengths and data
        message = struct.pack('>H', 2)  # 2 packets
        message += struct.pack('>H', len(packet1))  # length of packet1
        message += packet1
        message += struct.pack('>H', len(packet2))  # length of packet2
        message += packet2
        
        # Parse and verify
        ticks = self.parser.parse_binary_message(message)
        self.assertEqual(len(ticks), 2)
        self.assertEqual(ticks[0].instrument_token, 1234)
        self.assertEqual(ticks[0].last_price, 100.0)
        self.assertEqual(ticks[1].instrument_token, 5678)
        self.assertEqual(ticks[1].last_price, 200.0)
    
    def test_parse_index_packet(self):
        # Create index packet (NIFTY/SENSEX format)
        packet = struct.pack('>iiiiiiii', 
            256265,  # NIFTY token
            1500000, # last_price (15000.00)
            1510000, # high
            1490000, # low
            1495000, # open
            1480000, # close
            20000,   # change
            12345678 # timestamp
        )
        
        tick = self.parser._parse_index_packet(packet)
        self.assertIsNotNone(tick)
        self.assertEqual(tick.token, 256265)
        self.assertEqual(tick.last_price, 15000.0)
        self.assertEqual(tick.high_price, 15100.0)
        self.assertEqual(tick.change, 200.0)

class TestZerodhaWebSocketClient(unittest.TestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="test_token",
            user_id="test_user",
            api_key="test_key"
        )
        self.client.stop_event = Mock()
        self.client.stop_event.is_set.return_value = False
    
    @patch('websockets.connect')
    @patch('asyncio.sleep')
    async def test_connect_websocket(self, mock_sleep, mock_connect):
        # Setup mock websocket
        mock_ws = MagicMock()
        mock_connect.return_value.__aenter__.return_value = mock_ws
        
        # Mock responses
        mock_ws.recv.side_effect = [
            json.dumps({"type": "instruments_meta", "data": {"count": 100}}),
            json.dumps({"type": "app_code", "timestamp": "2023-01-01"}),
            b'\x00'  # heartbeat
        ]
        
        # Run test
        await self.client._connect_websocket()
        
        # Verify connection flow
        mock_connect.assert_called_once()
        mock_ws.send.assert_called()  # Should have sent subscribe messages
    
    @patch('asyncio.sleep')
    async def test_message_loop(self, mock_sleep):
        # Setup
        mock_ws = MagicMock()
        self.client.data_queue = MagicMock()
        
        # Test binary message
        test_packet = struct.pack('>ii', 1234, 10000)  # token=1234, price=100.00
        binary_msg = struct.pack('>H', 1)  # 1 packet
        binary_msg += struct.pack('>H', len(test_packet))
        binary_msg += test_packet
        
        # Test text message
        text_msg = json.dumps({"type": "order", "data": {}})
        
        mock_ws.recv.side_effect = [
            binary_msg,
            text_msg,
            RuntimeError("break loop")  # to exit the test
        ]
        
        # Run test (should break on the RuntimeError)
        with self.assertRaises(RuntimeError):
            await self.client._message_loop(mock_ws)
        
        # Verify messages were processed
        self.client.data_queue.put.assert_called_once()
        self.assertEqual(self.client.data_queue.put.call_args[0][0].instrument_token, 1234)

class TestKiteTokenWatcher(unittest.TestCase):
    @patch('pkbrokers.kite.instruments.KiteInstruments')
    @patch('pkbrokers.kite.ticks.ZerodhaWebSocketClient')
    def test_watch(self, mock_client, mock_instruments):
        # Setup
        watcher = KiteTokenWatcher(tokens=[1234, 5678])
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        # Test
        watcher.watch()
        
        # Verify
        mock_client.assert_called_once()
        mock_client_instance.start.assert_called_once()
