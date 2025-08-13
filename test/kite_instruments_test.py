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

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from pkbrokers.kite.instruments import Instrument, KiteInstruments


class TestKiteInstruments(unittest.TestCase):
    def setUp(self):
        # Create temp database for testing
        self.db_fd, self.db_path = tempfile.mkstemp()
        self.api_key = "test_api_key"
        self.access_token = "test_access_token"
        self.instruments = KiteInstruments(
            api_key=self.api_key, access_token=self.access_token, db_path=self.db_path
        )

        # Sample test data
        self.sample_instruments = [
            Instrument(
                instrument_token=12345,
                exchange_token="TEST1",
                tradingsymbol="RELIANCE",
                name="Reliance Industries",
                last_price=2500.50,
                expiry=None,
                strike=None,
                tick_size=0.05,
                lot_size=1,
                instrument_type="EQ",
                segment="NSE",
                exchange="NSE",
                last_updated=datetime.now().isoformat(),
            ),
            Instrument(
                instrument_token=67890,
                exchange_token="TEST2",
                tradingsymbol="NIFTY50",
                name="Nifty 50 Index",
                last_price=18000.75,
                expiry=None,
                strike=None,
                tick_size=0.05,
                lot_size=75,
                instrument_type="EQ",
                segment="INDICES",
                exchange="NSE",
                last_updated=datetime.now().isoformat(),
            ),
        ]

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_db_initialization(self):
        """Test database schema is properly initialized"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            self.assertIn("instruments", tables)

            # Verify indexes
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in cursor.fetchall()]
            self.assertIn("idx_instrument_token", indexes)
            self.assertIn("idx_tradingsymbol_segment", indexes)

    @patch("requests.get")
    def test_fetch_instruments(self, mock_get):
        """Test instrument fetching from API"""
        # Mock API response
        mock_response = MagicMock()
        mock_response.content = """instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,instrument_type,segment,exchange
12345,TEST1,RELIANCE,Reliance Industries,2500.50,,,0.05,1,EQ,NSE,NSE
67890,TEST2,NIFTY50,Nifty 50 Index,18000.75,,,0.05,75,IND,INDICES,NSE
""".encode("utf-8")
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        instruments = self.instruments.fetch_instruments()
        self.assertEqual(len(instruments), 2)
        self.assertEqual(instruments[0].tradingsymbol, "RELIANCE")
        self.assertEqual(instruments[1].tradingsymbol, "NIFTY50")

    def test_store_instruments(self):
        """Test instrument storage"""
        self.instruments.store_instruments(self.sample_instruments)
        count = self.instruments.get_instrument_count()
        self.assertEqual(count, 2)

        # Test update case
        updated_instruments = [
            Instrument(
                instrument_token=12345,
                exchange_token="UPDATED",
                tradingsymbol="RELIANCE",
                name="Reliance Industries Updated",
                last_price=2550.75,
                expiry=None,
                strike=None,
                tick_size=0.05,
                lot_size=1,
                instrument_type="EQ",
                segment="NSE",
                exchange="NSE",
                last_updated=datetime.now().isoformat(),
            )
        ]
        self.instruments.store_instruments(updated_instruments)

        # Verify update
        instrument = self.instruments.get_instrument(12345)
        self.assertEqual(instrument["name"], "Reliance Industries Updated")
        self.assertEqual(instrument["exchange_token"], "UPDATED")
        self.assertEqual(
            self.instruments.get_instrument_count(), 2
        )  # Should update, not add

    def test_get_equities(self):
        """Test equity filtering"""
        self.instruments.store_instruments(self.sample_instruments)

        # Get NSE equities
        equities = self.instruments.get_equities(segment="NSE")
        self.assertEqual(len(equities), 1)
        self.assertEqual(equities[0]["tradingsymbol"], "RELIANCE")

        # Get indices
        indices = self.instruments.get_equities(segment="INDICES")
        self.assertEqual(len(indices), 1)
        self.assertEqual(indices[0]["tradingsymbol"], "NIFTY50")

    def test_normalize_instrument(self):
        """Test data normalization"""
        raw_data = {
            "instrument_token": "12345",
            "exchange_token": "TEST1",
            "tradingsymbol": "  RELIANCE  ",
            "name": "  Reliance Industries  ",
            "last_price": "2500.50",
            "expiry": "",
            "strike": "",
            "tick_size": "0.05",
            "lot_size": "1",
            "instrument_type": "EQ",
            "segment": "NSE",
            "exchange": "NSE",
        }

        instrument = self.instruments._normalize_instrument(raw_data)
        self.assertIsNotNone(instrument)
        self.assertEqual(instrument.tradingsymbol, "RELIANCE")
        self.assertEqual(instrument.name, "Reliance Industries")
        self.assertIsNone(instrument.expiry)
        self.assertIsNone(instrument.strike)

    def test_normalize_expiry(self):
        """Test expiry date normalization"""
        self.assertIsNone(self.instruments._normalize_expiry(None))
        self.assertIsNone(self.instruments._normalize_expiry(""))
        self.assertEqual(self.instruments._normalize_expiry("2023-12-31"), "2023-12-31")
        self.assertIsNone(self.instruments._normalize_expiry("invalid-date"))

    def test_refresh_logic(self):
        # Test empty database
        self.assertTrue(self.instruments._needs_refresh())

        # Test fresh database
        self.instruments.sync_instruments()
        self.assertFalse(self.instruments._needs_refresh())

        # Test stale data (exactly 24 hours old)
        test_time = datetime.now(timezone.utc) - timedelta(hours=24)
        with self.instruments._get_connection() as conn:
            conn.execute("UPDATE instruments SET last_updated=?", (test_time,))
        self.assertTrue(self.instruments._needs_refresh())

        # Test same-day but old data
        test_time = datetime.now(timezone.utc).replace(hour=0, minute=0)
        with self.instruments._get_connection() as conn:
            conn.execute("UPDATE instruments SET last_updated=?", (test_time,))
        self.assertTrue(self.instruments._needs_refresh())
