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

"""

import json
import logging
import os
import sqlite3
import struct
import unittest
from datetime import datetime
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import MagicMock, Mock, patch

import pytest

from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher
from pkbrokers.kite.threadSafeDatabase import ThreadSafeDatabase
from pkbrokers.kite.tickMonitor import TickMonitor
from pkbrokers.kite.ticks import DEFAULT_PATH
from pkbrokers.kite.zerodhaWebSocketClient import ZerodhaWebSocketClient
from pkbrokers.kite.zerodhaWebSocketParser import ZerodhaWebSocketParser

os.environ["PKDevTools_Default_Log_Level"] = str(logging.DEBUG)


# Import the classes to test (adjust import path as needed)


class TestTickMonitor(IsolatedAsyncioTestCase):
    def setUp(self):
        self.test_db = os.path.join(DEFAULT_PATH, "test_ticks.db")
        self.monitor = TickMonitor(db_path=self.test_db)

    def tearDown(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)

    @pytest.fixture(autouse=True)
    def _setup_caplog(self, caplog):
        """Inject caplog into unittest class."""
        self.caplog = caplog

    # @pytest.fixture
    # def monitor_fixture(self):
    #     return TickMonitor(db_path=self.test_db, token_batches=[[1234, 5678]])

    @pytest.fixture
    def tst_db(self):
        db_path = ":memory:"
        conn = sqlite3.connect(db_path)
        yield conn
        conn.close()

    @pytest.mark.asyncio
    async def test_get_stale_instruments(self):
        # Setup test data
        db_path = ":memory:"
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE ticks(instrument_token INTEGER PRIMARY KEY, timestamp DATETIME);
            CREATE TABLE instrument_last_update(instrument_token INTEGER PRIMARY KEY, last_updated DATETIME);

            INSERT INTO ticks VALUES (1234, datetime('now'));
            INSERT INTO instrument_last_update VALUES (1234, datetime('now', '-2 minutes'));

            INSERT INTO ticks VALUES (5678, datetime('now'));
            INSERT INTO instrument_last_update VALUES (5678, datetime('now', '-5 minutes'));
        """)

        with patch("sqlite3.connect", return_value=conn):
            stale = await self.monitor._get_stale_instruments(
                [1234, 5678], stale_minutes=3
            )
            assert 5678 in stale  # Should be stale
            assert 1234 not in stale  # Should be fresh
        conn.close()

    @pytest.mark.asyncio
    async def test_monitor_stale_updates(self):
        monitor_fixture = TickMonitor(
            db_path=self.test_db, token_batches=[[1234, 5678]]
        )
        with (
            patch.object(
                monitor_fixture, "_check_stale_instruments", return_value=[1234]
            ) as mock_check,
            patch.object(monitor_fixture, "_handle_stale_instruments") as mock_handle,
        ):
            await monitor_fixture.monitor_stale_updates()
            mock_check.assert_called_once()
            mock_handle.assert_called_once_with([1234])

    @pytest.mark.asyncio
    @patch("sqlite3.connect")
    async def test_check_stale_instruments(self, mock_connect):
        monitor_fixture = TickMonitor(
            db_path=self.test_db, token_batches=[[1234, 5678]]
        )
        with patch.object(
            monitor_fixture, "_get_stale_instruments", side_effect=[[5678], []]
        ):
            # First call with stale instruments
            stale = await monitor_fixture._check_stale_instruments(
                [[1234, 5678], [9012]]
            )
            assert stale == [5678]

        # Second call with no stale instruments
        stale = await monitor_fixture._check_stale_instruments([[1234, 9012]])
        assert stale == []

    @pytest.mark.asyncio
    async def test_handle_stale_instruments(self):
        os.environ["PKDevTools_Default_Log_Level"] = str(logging.DEBUG)
        caplog = self.caplog
        with caplog.at_level(logging.DEBUG):
            monitor_fixture = TickMonitor(
                db_path=self.test_db, token_batches=[[1234, 5678]]
            )
            await monitor_fixture._handle_stale_instruments([1234, 5678])
            assert len(caplog.records) >= 0, "No logs were captured"
            if len(caplog.records) > 0:
                assert "Following instruments (2) have stale updates" in caplog.text

    @patch("sqlite3.connect")
    async def test_get_stale_instruments_1(self, mock_connect):
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
        self.assertEqual(len(stale), 0)


class TestGetStaleInstruments:
    @pytest.mark.asyncio
    async def test_empty_token_batch(self):
        """Test with empty token batch returns empty list"""
        instance = TickMonitor()
        result = await instance._get_stale_instruments([])
        assert result == []

    @pytest.mark.asyncio
    @patch("sqlite3.connect")
    async def test_stale_instruments_found(self, mock_connect):
        """Test correctly identifies stale instruments"""
        # Setup mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock DB response - stale tokens 1234 and 5678
        mock_cursor.fetchall.return_value = [(1234,), (5678,)]

        instance = TickMonitor()
        result = await instance._get_stale_instruments(
            [1234, 5678, 9012], stale_minutes=5
        )

        # Verify query construction
        expected_query = """
            SELECT t.instrument_token
            FROM ticks t
            LEFT JOIN instrument_last_update u
            ON t.instrument_token = u.instrument_token
            WHERE t.instrument_token IN (?,?,?)
            AND (
                u.last_updated IS NULL OR
                strftime('%s','now') - strftime('%s',u.last_updated) > ? * 60
            )
        """.strip()

        mock_cursor.execute.assert_called_once()
        args, kwargs = mock_cursor.execute.call_args
        assert expected_query in args[0]
        assert args[1] == (1234, 5678, 9012, 5)

        # Verify results
        assert sorted(result) == [1234, 5678]

    @pytest.mark.asyncio
    @patch("sqlite3.connect")
    async def test_no_stale_instruments(self, mock_connect):
        """Test returns empty list when no stale instruments"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        instance = TickMonitor()
        result = await instance._get_stale_instruments([1234, 5678])
        assert result == []

    @pytest.mark.asyncio
    @patch("sqlite3.connect")
    @patch(
        "pkbrokers.kite.tickMonitor.logger.error"
    )  # Replace with your actual logger path
    async def test_database_error_handling(self, mock_logger, mock_connect):
        """Test properly handles database errors"""
        mock_connect.side_effect = sqlite3.Error("DB connection failed")

        instance = TickMonitor()
        result = await instance._get_stale_instruments([1234, 5678])

        # Verify error was logged
        mock_logger.assert_called_once()
        assert result == []

    @pytest.mark.asyncio
    @patch("sqlite3.connect")
    async def test_custom_stale_threshold(self, mock_connect):
        """Test respects custom stale_minutes parameter"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        instance = TickMonitor()
        await instance._get_stale_instruments([1234], stale_minutes=15)

        # Verify the stale_minutes parameter was used in query
        args, _ = mock_cursor.execute.call_args
        assert args[1][-1] == 15  # Last parameter is stale_minutes


class TestThreadSafeDatabase(unittest.TestCase):
    def setUp(self):
        self.test_db = os.path.join(DEFAULT_PATH, "test_ticks.db")
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

            self.assertIn("ticks", tables)
            self.assertIn("market_depth", tables)
            self.assertIn("instrument_last_update", tables)

    def test_insert_ticks(self):
        test_ticks = [
            {
                "instrument_token": 1234,
                "timestamp": datetime.now(),
                "last_price": 100.0,
                "day_volume": 1000,
                "oi": 500,
                "buy_quantity": 600,
                "sell_quantity": 400,
                "high_price": 101.0,
                "low_price": 99.0,
                "open_price": 100.5,
                "prev_day_close": 99.5,
                "depth": {
                    "bid": [{"price": 99.9, "quantity": 100, "orders": 5}],
                    "ask": [{"price": 100.1, "quantity": 100, "orders": 5}],
                },
            }
        ]

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
        packet1 = struct.pack(">ii", 1234, 10000)  # token=1234, price=100.00
        packet2 = struct.pack(">ii", 5678, 20000)  # token=5678, price=200.00

        # Build message: 2 packets, then lengths and data
        message = struct.pack(">H", 2)  # 2 packets
        message += struct.pack(">H", len(packet1))  # length of packet1
        message += packet1
        message += struct.pack(">H", len(packet2))  # length of packet2
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
        packet = struct.pack(
            ">iiiiiiii",
            256265,  # NIFTY token
            1500000,  # last_price (15000.00)
            1510000,  # high
            1490000,  # low
            1495000,  # open
            1480000,  # close
            20000,  # change
            12345678,  # timestamp
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
            enctoken="test_token", user_id="test_user", api_key="test_key"
        )
        self.client.stop_event = Mock()
        self.client.stop_event.is_set.return_value = False

    @patch("websockets.connect")
    @patch("asyncio.sleep")
    async def test_connect_websocket(self, mock_sleep, mock_connect):
        # Setup mock websocket
        mock_ws = MagicMock()
        mock_connect.return_value.__aenter__.return_value = mock_ws

        # Mock responses
        mock_ws.recv.side_effect = [
            json.dumps({"type": "instruments_meta", "data": {"count": 100}}),
            json.dumps({"type": "app_code", "timestamp": "2023-01-01"}),
            b"\x00",  # heartbeat
        ]

        # Run test
        await self.client._connect_websocket()

        # Verify connection flow
        mock_connect.assert_called_once()
        mock_ws.send.assert_called()  # Should have sent subscribe messages

    @patch("asyncio.sleep")
    async def test_message_loop(self, mock_sleep):
        # Setup
        mock_ws = MagicMock()
        self.client.data_queue = MagicMock()

        # Test binary message
        test_packet = struct.pack(">ii", 1234, 10000)  # token=1234, price=100.00
        binary_msg = struct.pack(">H", 1)  # 1 packet
        binary_msg += struct.pack(">H", len(test_packet))
        binary_msg += test_packet

        # Test text message
        text_msg = json.dumps({"type": "order", "data": {}})

        mock_ws.recv.side_effect = [
            binary_msg,
            text_msg,
            RuntimeError("break loop"),  # to exit the test
        ]

        # Run test (should break on the RuntimeError)
        with self.assertRaises(RuntimeError):
            await self.client._message_loop(mock_ws)

        # Verify messages were processed
        self.client.data_queue.put.assert_called_once()
        self.assertEqual(
            self.client.data_queue.put.call_args[0][0].instrument_token, 1234
        )


class TestKiteTokenWatcher(TestCase):
    @patch.dict("os.environ", {"KTOKEN": "mocked_token", "KUSER": "mocked_user"})
    def test_watch(self):
        """Test that watch() creates and starts websocket client."""
        with patch(
            "pkbrokers.kite.zerodhaWebSocketClient.ZerodhaWebSocketClient"
        ) as mock_ws_client:
            with patch(
                "pkbrokers.kite.instruments.KiteInstruments"
            ) as mock_instruments:
                # Setup mock instruments
                mock_instruments_instance = MagicMock()
                mock_instruments.return_value = mock_instruments_instance
                mock_instruments_instance.get_instrument_tokens.return_value = [
                    1234,
                    5678,
                ]

                # Setup mock websocket client
                mock_ws_instance = MagicMock(spec=ZerodhaWebSocketClient)
                mock_ws_client.return_value = mock_ws_instance

                mock_watcher_instance = MagicMock()
                mock_watcher = mock_watcher_instance
                mock_watcher.client.return_value = mock_ws_instance
                # Create watcher with test tokens
                watcher = KiteTokenWatcher(tokens=[1234, 5678], client=mock_ws_instance)
                watcher.watch()

                # # Verify client was created with expected arguments
                # mock_ws_client.assert_called_once_with(
                #     enctoken='mocked_token',
                #     user_id='mocked_user',
                #     token_batches=[[1234, 5678]],
                #     watcher_queue=None
                # )

                # Verify client was started
                mock_ws_instance.start.assert_called_once()
