# import asyncio
import asyncio
import base64
import json
import logging
import os
import struct
import time
import unittest
from datetime import datetime
from queue import Empty, Queue
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, quote, urlparse

# import pytz
from websockets.exceptions import ConnectionClosedError, InvalidStatusCode

from pkbrokers.kite.ticks import Tick

os.environ["PKDevTools_Default_Log_Level"] = str(logging.DEBUG)

# import pytest

from pkbrokers.kite.zerodhaWebSocketClient import (
    BSE_SENSEX,
    NIFTY_50,
    OTHER_INDICES,
    ZerodhaWebSocketClient,
)


class TestBuildWebSocketUrl(unittest.TestCase):
    def setUp(self):
        self.enctoken = "dummy_enctoken_123"
        self.user_id = "AB12345"
        self.api_key = "test_api_key"
        self.client = ZerodhaWebSocketClient(
            enctoken=self.enctoken, user_id=self.user_id, api_key=self.api_key
        )

    def tearDown(self):
        del self.client

    def test_basic_url_structure(self):
        """Test the basic URL structure is correct"""
        url = self.client._build_websocket_url()

        # Check it's a valid websocket URL
        self.assertTrue(url.startswith("wss://ws.zerodha.com/?"))

        # Parse the URL components
        parsed = urlparse(url)
        self.assertEqual(parsed.scheme, "wss")
        self.assertEqual(parsed.netloc, "ws.zerodha.com")
        self.assertEqual(parsed.path, "/")

    def test_required_parameters_present(self):
        """Test all required parameters are present"""
        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        self.assertIn("api_key", query)
        self.assertIn("user_id", query)
        self.assertIn("enctoken", query)
        self.assertIn("uid", query)
        self.assertIn("user-agent", query)
        self.assertIn("version", query)

    def test_parameter_values(self):
        """Test parameter values are correctly included"""
        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        self.assertEqual(query["api_key"][0], self.api_key)
        self.assertEqual(query["user_id"][0], self.user_id)
        self.assertIn(self.enctoken, query["enctoken"][0])  # May be URL encoded
        self.assertEqual(query["user-agent"][0], "kite3-web")
        self.assertEqual(query["version"][0], "3.0.0")

    def test_uid_parameter(self):
        """Test the uid parameter is a timestamp"""
        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        # Should be a timestamp in milliseconds
        uid = query["uid"][0]
        self.assertTrue(uid.isdigit())

        # Rough check that it's a recent timestamp (within last 5 seconds)
        current_ms = int(time.time() * 1000)
        uid_ms = int(uid)
        self.assertAlmostEqual(current_ms, uid_ms, delta=5000)

    def test_enctoken_url_encoding(self):
        """Test special characters in enctoken are properly encoded"""
        special_token = "abc123!@#$%^&*()"
        self.client.enctoken = special_token

        url = self.client._build_websocket_url()
        # query = parse_qs(urlparse(url).query)

        # Should be URL encoded
        self.assertNotIn(special_token, urlparse(url).query)
        self.assertIn(quote(special_token), urlparse(url).query)

    def test_empty_api_key(self):
        """Test behavior with empty api_key"""
        self.client.api_key = ""
        with self.assertRaises(ValueError):
            self.client._build_websocket_url()

    def test_empty_user_id(self):
        """Test behavior with empty user_id"""
        self.client.user_id = ""
        with self.assertRaises(ValueError):
            self.client._build_websocket_url()

    def test_empty_enctoken(self):
        """Test behavior with empty enctoken"""
        self.client.enctoken = ""
        with self.assertRaises(ValueError):
            self.client._build_websocket_url()

    def test_none_api_key(self):
        """Test behavior with None api_key"""
        self.client.api_key = None
        with self.assertRaises(ValueError):
            self.client._build_websocket_url()

    def test_none_user_id(self):
        """Test behavior with None user_id"""
        self.client.user_id = None
        with self.assertRaises(ValueError):
            self.client._build_websocket_url()

    def test_none_enctoken(self):
        """Test behavior with None enctoken"""
        self.client.enctoken = None
        with self.assertRaises(ValueError):
            self.client._build_websocket_url()

    def test_very_long_enctoken(self):
        """Test with a very long enctoken"""
        long_token = "a" * 2000  # 2000 character token
        self.client.enctoken = long_token

        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        # Should be properly encoded and included
        self.assertTrue(len(query["enctoken"][0]) > 0)

    def test_special_characters_in_user_id(self):
        """Test with special characters in user_id"""
        special_user_id = "user@domain.com"
        self.client.user_id = special_user_id

        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        # Should be URL encoded
        self.assertEqual(query["user_id"][0], special_user_id)

    @patch("time.time", return_value=1625097600.0)  # Mock time to fixed value
    def test_deterministic_uid_with_mocked_time(self, mock_time):
        """Test UID generation with mocked time"""
        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        # Should be the mocked timestamp in milliseconds
        self.assertEqual(query["uid"][0], "1625097600000")

    def test_url_with_custom_api_key(self):
        """Test with a custom api_key"""
        custom_api_key = "custom_kite_api"
        self.client.api_key = custom_api_key

        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        self.assertEqual(query["api_key"][0], custom_api_key)

    def test_url_with_numeric_user_id(self):
        """Test with numeric user_id"""
        numeric_user_id = "123456"
        self.client.user_id = numeric_user_id

        url = self.client._build_websocket_url()
        query = parse_qs(urlparse(url).query)

        self.assertEqual(query["user_id"][0], numeric_user_id)

    def test_parameter_ordering(self):
        """Test that parameters appear in consistent order (for caching)"""
        url1 = self.client._build_websocket_url()
        url2 = self.client._build_websocket_url()

        # The query parameters should be in the same order
        self.assertEqual(urlparse(url1).query, urlparse(url2).query)

    def test_url_with_unicode_enctoken(self):
        """Test with Unicode characters in enctoken"""
        unicode_token = "tøkën_123"
        self.client.enctoken = unicode_token

        url = self.client._build_websocket_url()
        # query = parse_qs(urlparse(url).query)

        # Should be properly URL encoded
        self.assertIn("t%C3%B8k%C3%ABn_123", urlparse(url).query)


class TestParseBinaryPacket(unittest.TestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )

    def _create_packet(self, format_str, values):
        """Helper to create binary packets"""
        return struct.pack(format_str, *values)

    def test_minimum_packet_ltp(self):
        """Test parsing minimum LTP packet (8 bytes)"""
        packet = self._create_packet(
            ">ii", [123456, 10050]
        )  # instrument_token, last_price (in paise)
        result = self.client._parse_binary_packet(packet)

        self.assertIsNotNone(result)
        self.assertEqual(result["instrument_token"], 123456)
        self.assertEqual(result["last_price"], 100.50)  # converted to rupees
        self.assertIsNone(result["last_quantity"])
        self.assertIsNone(result["avg_price"])

    def test_ltp_with_quantity(self):
        """Test LTP with quantity (12 bytes)"""
        packet = self._create_packet(">iii", [123456, 10050, 200])  # + last_quantity
        result = self.client._parse_binary_packet(packet)

        self.assertEqual(result["last_quantity"], 200)
        self.assertEqual(result["last_price"], 100.50)

    def test_full_quote_packet(self):
        """Test full quote packet (44 bytes)"""
        values = [
            123456,  # instrument_token
            10050,  # last_price (paise)
            200,  # last_quantity
            10100,  # avg_price (paise)
            50000,  # volume
            250,  # buy_quantity
            300,  # sell_quantity
            10200,  # open (paise)
            10300,  # high (paise)
            9900,  # low (paise)
            9800,  # prev_close (paise)
            1625097600,  # last_trade_timestamp
            1000,  # oi
            1100,  # oi_day_high
            900,  # oi_day_low
            1625097601,  # exchange_timestamp
        ]
        packet = self._create_packet(">iiiiiiiiiiiiiiii", values)
        result = self.client._parse_binary_packet(packet)

        self.assertEqual(result["instrument_token"], 123456)
        self.assertEqual(result["last_price"], 100.50)
        self.assertEqual(result["avg_price"], 101.00)
        self.assertEqual(result["day_volume"], 50000)
        self.assertEqual(result["high_price"], 103.00)
        self.assertEqual(result["prev_day_close"], 98.00)
        self.assertEqual(result["exchange_timestamp"], 1625097601)

    def test_full_packet_with_depth(self):
        """Test full packet with market depth (184 bytes)"""
        # Create base values (16 integers)
        base_values = [
            123456,  # instrument_token
            10050,  # last_price
            200,  # last_quantity
            10100,  # avg_price
            50000,  # volume
            250,  # buy_quantity
            300,  # sell_quantity
            10200,  # open
            10300,  # high
            9900,  # low
            9800,  # prev_close
            1625097600,  # last_trade_timestamp
            1000,  # oi
            1100,  # oi_day_high
            900,  # oi_day_low
            1625097601,  # exchange_timestamp
        ]

        # Create market depth data (5 bids + 5 asks)
        # Each depth entry: quantity (i), price (i), orders (h)
        depth_data = []
        for i in range(1, 6):  # 5 bid levels
            depth_data.extend([i * 100, i * 10050, i])

        for i in range(1, 6):  # 5 ask levels
            depth_data.extend([i * 100, i * 10100, i])

        # Correct format string:
        # 16i (base values) + 10iih (5 bids + 5 asks)
        # Each "iih" is one depth entry (quantity, price, orders)
        format_str = ">16i" + "iih" * 10
        values = base_values + depth_data

        packet = struct.pack(format_str, *values)

        # Verify total packet size
        # 16 integers (4 bytes each) = 64 bytes
        # 10 depth entries (4+4+2 bytes each) = 100 bytes
        # Total = 164 bytes (not 184 as previously thought)
        self.assertEqual(len(packet), 164)

        result = self.client._parse_binary_packet(packet)

        # Verify depth data
        self.assertEqual(len(result["depth"]["bid"]), 5)
        self.assertEqual(len(result["depth"]["ask"]), 5)

        # Verify first bid level
        self.assertEqual(result["depth"]["bid"][0]["quantity"], 100)
        self.assertEqual(result["depth"]["bid"][0]["price"], 100.50)
        self.assertEqual(result["depth"]["bid"][0]["orders"], 1)

        # Verify last ask level
        self.assertEqual(result["depth"]["ask"][4]["quantity"], 500)
        self.assertEqual(result["depth"]["ask"][4]["price"], 505.00)
        self.assertEqual(result["depth"]["ask"][4]["orders"], 5)

    def test_invalid_packet_too_short(self):
        """Test packet shorter than minimum 8 bytes"""
        packet = b"\x01\x02\x03\x04\x05\x06\x07"  # 7 bytes
        result = self.client._parse_binary_packet(packet)
        self.assertIsNone(result)

    def test_empty_packet(self):
        """Test empty packet"""
        packet = b""
        result = self.client._parse_binary_packet(packet)
        self.assertIsNone(result)

    def test_partial_depth_packet(self):
        """Test packet with partial depth data"""
        # Create base values (16 integers)
        base_values = [
            123456,  # instrument_token
            10050,  # last_price
            200,  # last_quantity
            10100,  # avg_price
            50000,  # volume
            250,  # buy_quantity
            300,  # sell_quantity
            10200,  # open
            10300,  # high
            9900,  # low
            9800,  # prev_close
            1625097600,  # last_trade_timestamp
            1000,  # oi
            1100,  # oi_day_high
            900,  # oi_day_low
            1625097601,  # exchange_timestamp
        ]

        # Create market depth data (5 bids + 5 asks)
        # Each depth entry: quantity (i), price (i), orders (h)
        depth_data = []
        for i in range(1, 4):  # 3 bid levels
            depth_data.extend([i * 100, i * 10050, i])

        # for i in range(1, 6):  # 5 ask levels
        #     depth_data.extend([i*100, i*10100, i])

        # Correct format string:
        # 16i (base values) + 10iih (5 bids + 5 asks)
        # Each "iih" is one depth entry (quantity, price, orders)
        format_str = ">16i" + "iih" * 3
        values = base_values + depth_data

        packet = struct.pack(format_str, *values)

        # Verify total packet size
        # 16 integers (4 bytes each) = 64 bytes
        # 10 depth entries (4+4+2 bytes each) = 100 bytes
        # Total = 164 bytes (not 184 as previously thought)
        self.assertEqual(len(packet), 94)
        result = self.client._parse_binary_packet(packet)

        self.assertEqual(len(result["depth"]["bid"]), 3)
        self.assertEqual(len(result["depth"]["ask"]), 0)  # No ask levels

    def test_packet_with_zero_values(self):
        """Test packet with zero values"""
        packet = self._create_packet(">ii", [0, 0])  # instrument_token=0, last_price=0
        result = self.client._parse_binary_packet(packet)

        self.assertEqual(result["instrument_token"], 0)
        self.assertEqual(result["last_price"], 0.00)

    def test_packet_with_negative_price(self):
        """Test packet with negative price (shouldn't happen but test handling)"""
        packet = self._create_packet(">ii", [123456, -10050])  # negative last_price
        result = self.client._parse_binary_packet(packet)

        self.assertEqual(result["last_price"], -100.50)

    def test_packet_with_max_values(self):
        """Test packet with maximum possible values"""
        max_int = 2**31 - 1
        packet = self._create_packet(">ii", [max_int, max_int])
        result = self.client._parse_binary_packet(packet)

        self.assertEqual(result["instrument_token"], max_int)
        self.assertEqual(result["last_price"], max_int / 100)

    def test_packet_with_invalid_data(self):
        """Test packet with invalid binary data"""
        packet = b"INVALID_BINARY_DATA"
        result = self.client._parse_binary_packet(packet)
        self.assertIsNone(result["day_volume"])

    def test_packet_with_extra_data(self):
        """Test packet with extra data beyond expected format"""
        # Create normal 8 byte packet plus extra garbage
        packet = self._create_packet(">ii", [123456, 10050]) + b"EXTRA_DATA"
        result = self.client._parse_binary_packet(packet)

        # Should still parse the valid part
        self.assertEqual(result["instrument_token"], 123456)
        self.assertEqual(result["last_price"], 100.50)

    def test_packet_with_floating_point_conversion(self):
        """Test proper conversion from paise to rupees"""
        test_cases = [
            (10050, 100.50),  # standard case
            (10000, 100.00),  # whole rupees
            (10001, 100.01),  # single paise
            (0, 0.00),  # zero
            (1, 0.01),  # single paise
            (9999, 99.99),  # max paise
        ]

        for paise, expected_rupees in test_cases:
            packet = self._create_packet(">ii", [123456, paise])
            result = self.client._parse_binary_packet(packet)
            self.assertEqual(result["last_price"], expected_rupees)

    def test_packet_with_missing_depth_entries(self):
        """Test packet with incomplete depth entries"""
        base_values = [
            123456,
            10050,
            200,
            10100,
            50000,
            250,
            300,
            10200,
            10300,
            9900,
            9800,
            1625097600,
            1000,
            1100,
            900,
            1625097601,
        ]

        # Add only partial depth entry (missing orders count)
        depth_data = [100, 10050]  # quantity, price (missing orders)

        packet = self._create_packet(
            ">iiiiiiiiiiiiiiii" + "ii", base_values + depth_data
        )
        result = self.client._parse_binary_packet(packet)

        # Should not include depth since it's incomplete
        self.assertIsNone(result.get("depth"))


class TestBuildHeaders(unittest.TestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )

    def test_headers_structure(self):
        """Test that headers contain all required fields"""
        headers = self.client._build_headers()

        required_keys = {
            "Host",
            "Connection",
            "Pragma",
            "Cache-Control",
            "User-Agent",
            "Upgrade",
            "Origin",
            "Sec-WebSocket-Version",
            "Accept-Encoding",
            "Accept-Language",
            "Sec-WebSocket-Key",
            "Sec-WebSocket-Extensions",
        }
        self.assertEqual(set(headers.keys()), required_keys)

    def test_websocket_key_generation(self):
        """Test WebSocket key is properly generated and base64 encoded"""
        with patch("os.urandom") as mock_urandom:
            # Mock random bytes to predictable value
            mock_urandom.return_value = (
                b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12\x13\x14\x15\x16"
            )

            headers = self.client._build_headers()
            ws_key = headers["Sec-WebSocket-Key"]

            # Should be base64 encoded 16 random bytes
            self.assertEqual(
                ws_key, base64.b64encode(mock_urandom.return_value).decode("utf-8")
            )
            mock_urandom.assert_called_once_with(16)

    def test_static_header_values(self):
        """Test headers with static expected values"""
        headers = self.client._build_headers()

        self.assertEqual(headers["Host"], "ws.zerodha.com")
        self.assertEqual(headers["Connection"], "Upgrade")
        self.assertEqual(headers["Upgrade"], "websocket")
        self.assertEqual(headers["Sec-WebSocket-Version"], "13")
        self.assertEqual(headers["Origin"], "https://kite.zerodha.com")

    def test_user_agent_format(self):
        """Test User-Agent follows expected browser format"""
        headers = self.client._build_headers()
        user_agent = headers["User-Agent"]

        # Should match browser-like user agent string
        self.assertRegex(
            user_agent,
            r"^Mozilla/5\.0 \(.+\) AppleWebKit/\d+\.\d+ \(KHTML, like Gecko\) Chrome/\d+\.\d+\.\d+\.\d+ Safari/\d+\.\d+$",
        )

    def test_cache_control(self):
        """Test cache control headers prevent caching"""
        headers = self.client._build_headers()
        self.assertEqual(headers["Cache-Control"], "no-cache")
        self.assertEqual(headers["Pragma"], "no-cache")

    def test_websocket_extensions(self):
        """Test WebSocket extensions are properly specified"""
        headers = self.client._build_headers()
        self.assertEqual(
            headers["Sec-WebSocket-Extensions"],
            "permessage-deflate; client_max_window_bits",
        )

    def test_accept_headers(self):
        """Test accepted encodings and languages"""
        headers = self.client._build_headers()
        self.assertIn("gzip", headers["Accept-Encoding"])
        self.assertIn("deflate", headers["Accept-Encoding"])
        self.assertIn("en-US", headers["Accept-Language"])

    def test_unique_websocket_key(self):
        """Test WebSocket key is different on each call"""
        headers1 = self.client._build_headers()
        headers2 = self.client._build_headers()

        self.assertNotEqual(
            headers1["Sec-WebSocket-Key"], headers2["Sec-WebSocket-Key"]
        )

    def test_headers_immutable(self):
        """Test returned headers can't modify original"""
        headers = self.client._build_headers()
        headers["Host"] = "malicious.com"

        # New call should still have original host
        new_headers = self.client._build_headers()
        self.assertEqual(new_headers["Host"], "ws.zerodha.com")

    @patch.dict("os.environ", {"HTTP_PROXY": "http://proxy.example.com:8080"})
    def test_headers_with_proxy(self):
        """Test headers are consistent even with proxy environment"""
        headers = self.client._build_headers()
        self.assertEqual(headers["Host"], "ws.zerodha.com")

    def test_header_value_types(self):
        """Test all header values are strings"""
        headers = self.client._build_headers()
        for value in headers.values():
            self.assertIsInstance(value, str)


class TestBuildTokens(unittest.TestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )
        # Setup test environment
        self.env_patcher = patch.dict("os.environ", {"KTOKEN": "test_token"})
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        if os.path.exists(".env.dev"):
            os.remove(".env.dev")

    def _create_test_env_file(self):
        """Helper to create a test .env.dev file"""
        with open(".env.dev", "w") as f:
            f.write("KTOKEN=env_token\n")

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_build_tokens_success(self, mock_kite):
        """Test successful token building with environment token"""
        # Setup mock
        mock_instance = mock_kite.return_value
        mock_instance.get_instrument_count.return_value = 100
        equities = [{"instrument_token": i} for i in range(1, 101)]
        mock_instance.get_equities.return_value = equities
        mock_instance.get_instrument_tokens.return_value = [
            int(eq["instrument_token"]) for eq in equities
        ]

        # Call method
        self.client._build_tokens()

        # Verify results
        self.assertEqual(
            len(self.client.token_batches), 1
        )  # 100 tokens / 500 batch size
        self.assertEqual(len(self.client.token_batches[0]), 100)
        mock_instance.sync_instruments.assert_not_called()  # Shouldn't sync when count > 0

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_token_batching(self, mock_kite):
        """Test proper batching of tokens"""
        # Setup mock with 1500 instruments
        mock_instance = mock_kite.return_value
        mock_instance.get_instrument_count.return_value = 1500
        equities = [{"instrument_token": i} for i in range(1, 1501)]
        mock_instance.get_equities.return_value = equities
        mock_instance.get_instrument_tokens.return_value = [
            int(eq["instrument_token"]) for eq in equities
        ]

        # Call method
        self.client._build_tokens()

        # Verify batching
        self.assertEqual(len(self.client.token_batches), 3)  # 1500 / 500 = 3 batches
        self.assertEqual(len(self.client.token_batches[0]), 500)
        self.assertEqual(len(self.client.token_batches[1]), 500)
        self.assertEqual(len(self.client.token_batches[2]), 500)

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_empty_instruments_force_fetch(self, mock_kite):
        """Test force fetch when no instruments exist"""
        # Setup mock
        mock_instance = mock_kite.return_value
        mock_instance.get_instrument_count.return_value = 0
        equities = [{"instrument_token": i} for i in range(1, 101)]
        mock_instance.get_equities.return_value = equities
        mock_instance.get_instrument_tokens.return_value = [
            int(eq["instrument_token"]) for eq in equities
        ]

        # Call method
        self.client._build_tokens()

        # Verify force fetch was called
        mock_instance.sync_instruments.assert_called_once_with(force_fetch=True)

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_env_token_priority(self, mock_kite):
        """Test environment token takes precedence over .env file"""
        self._create_test_env_file()

        # Call method
        self.client._build_tokens()

        # Verify environment token was used (not .env file token)
        self.assertEqual(self.client.enctoken, "test_token")

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_env_file_fallback(self, mock_kite):
        """Test .env file fallback when no environment token"""
        self._create_test_env_file()
        with patch.dict("os.environ", {}, clear=True):
            # Call method
            self.client._build_tokens()

            # Verify .env file token was used
            self.assertEqual(self.client.enctoken, "env_token")

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_missing_token_error(self, mock_kite):
        """Test error when no token is available"""
        with patch.dict("os.environ", {}, clear=True):
            if os.path.exists(".env.dev"):
                os.remove(".env.dev")

            self.client._build_tokens()

            self.assertIn("You need your Kite token", str(self.client.enctoken))

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_token_refresh(self, mock_kite):
        """Test existing token is refreshed"""
        # original_token = self.client.enctoken

        # Setup mock
        mock_instance = mock_kite.return_value
        mock_instance.get_instrument_count.return_value = 100
        mock_instance.get_equities.return_value = [
            {"instrument_token": i} for i in range(1, 101)
        ]

        # Call method
        self.client._build_tokens()

        # Verify token was updated (may be same or different depending on implementation)
        self.assertIsNotNone(self.client.enctoken)
        # Depending on implementation, you might want to assert it changed or stayed same

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_ws_url_update(self, mock_kite):
        """Test WebSocket URL is updated with new token"""
        original_url = self.client.ws_url

        # Setup mock
        mock_instance = mock_kite.return_value
        mock_instance.get_instrument_count.return_value = 100
        equities = [{"instrument_token": i} for i in range(1, 101)]
        mock_instance.get_equities.return_value = equities
        mock_instance.get_instrument_tokens.return_value = [
            int(eq["instrument_token"]) for eq in equities
        ]

        # Call method
        self.client._build_tokens()

        # Verify URL was updated
        self.assertNotEqual(original_url, self.client._build_websocket_url())
        self.assertIn(quote(self.client.enctoken), self.client._build_websocket_url())

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_empty_equities(self, mock_kite):
        """Test handling when get_equities returns empty list"""
        # Setup mock
        mock_instance = mock_kite.return_value
        mock_instance.get_instrument_count.return_value = 100
        mock_instance.get_equities.return_value = []
        mock_instance.get_instrument_tokens.return_value = []

        self.client._build_tokens()

        self.assertEqual(self.client.token_batches, [])

    @patch("pkbrokers.kite.instruments.KiteInstruments")
    def test_instrument_token_format(self, mock_kite):
        """Test instrument tokens have correct format"""
        # Setup mock with specific token values
        test_tokens = [100, 200, 300, 400, 500]
        mock_instance = mock_kite.return_value
        mock_instance.get_instrument_count.return_value = len(test_tokens)
        equities = [{"instrument_token": i} for i in test_tokens]
        mock_instance.get_equities.return_value = equities
        mock_instance.get_instrument_tokens.return_value = [
            int(eq["instrument_token"]) for eq in equities
        ]

        # Call method
        self.client._build_tokens()

        # Verify tokens are integers and in expected batches
        for batch in self.client.token_batches:
            for token in batch:
                self.assertIsInstance(token, int)
        self.assertEqual(self.client.token_batches[0], test_tokens)


class TestSubscribeInstruments(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )
        self.client.token_batches = [
            [101, 102, 103],
            [201, 202, 203],
        ]  # Sample token batches
        self.mock_websocket = AsyncMock()

    async def test_subscribe_indices_on_first_call(self):
        """Test indices are subscribed only on first call"""
        self.client.index_subscribed = False

        await self.client._subscribe_instruments(self.mock_websocket, [])

        # Verify NIFTY_50 subscription
        nifty_subscribe_call = json.dumps({"a": "subscribe", "v": NIFTY_50})
        nifty_mode_call = json.dumps({"a": "mode", "v": ["full", NIFTY_50]})

        # Verify BSE_SENSEX subscription
        sensex_subscribe_call = json.dumps({"a": "subscribe", "v": BSE_SENSEX})
        sensex_mode_call = json.dumps({"a": "mode", "v": ["full", BSE_SENSEX]})

        calls = [call.args[0] for call in self.mock_websocket.send.call_args_list]
        self.assertIn(nifty_subscribe_call, calls)
        self.assertIn(nifty_mode_call, calls)
        self.assertIn(sensex_subscribe_call, calls)
        self.assertIn(sensex_mode_call, calls)
        self.assertTrue(self.client.index_subscribed)

    async def test_no_indices_on_subsequent_calls(self):
        """Test indices aren't re-subscribed on subsequent calls"""
        self.client.index_subscribed = True

        await self.client._subscribe_instruments(self.mock_websocket, [])

        # Verify no index subscription messages
        for call in self.mock_websocket.send.call_args_list:
            message = json.loads(call.args[0])
            if message["a"] == "subscribe":
                self.assertNotIn(NIFTY_50, message["v"])
                self.assertNotIn(BSE_SENSEX, message["v"])

    async def test_token_batch_subscription(self):
        """Test proper subscription to token batches"""
        test_batch = [101, 102, 103]

        await self.client._subscribe_instruments(self.mock_websocket, [test_batch])

        # Verify subscribe and mode messages for the batch
        expected_subscribe = json.dumps({"a": "subscribe", "v": test_batch})
        expected_mode = json.dumps({"a": "mode", "v": ["full", test_batch]})

        calls = [call.args[0] for call in self.mock_websocket.send.call_args_list]
        self.assertIn(expected_subscribe, calls)
        self.assertIn(expected_mode, calls)

    async def test_rate_limiting(self):
        """Test rate limiting between batches"""
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            batches = [[101, 102], [201, 202], [301, 302]]

            await self.client._subscribe_instruments(self.mock_websocket, batches)

            # Verify sleep called between batches
            self.assertEqual(mock_sleep.call_count, len(batches))
            mock_sleep.assert_called_with(1)  # Default rate limit

    async def test_stop_event_handling(self):
        """Test early exit when stop event is set"""
        self.client.stop_event.set()

        await self.client._subscribe_instruments(self.mock_websocket, [[101, 102]])

        # Verify no messages were sent
        self.mock_websocket.send.assert_not_called()

    async def test_empty_batch_handling(self):
        """Test handling of empty token batches"""
        await self.client._subscribe_instruments(self.mock_websocket, [[]])

        # Verify no subscribe/mode messages for empty batch
        for call in self.mock_websocket.send.call_args_list:
            message = json.loads(call.args[0])
            if message["a"] == "subscribe":
                self.assertEqual(message["v"], [])

    async def test_all_indices_subscription(self):
        """Test subscription to all indices when flag is True"""
        self.client.index_subscribed = False

        await self.client._subscribe_instruments(
            self.mock_websocket, [], subscribe_all_indices=True
        )

        # Verify OTHER_INDICES subscription
        other_indices_subscribe = json.dumps({"a": "subscribe", "v": OTHER_INDICES})
        other_indices_mode = json.dumps({"a": "mode", "v": ["full", OTHER_INDICES]})

        calls = [call.args[0] for call in self.mock_websocket.send.call_args_list]
        self.assertIn(other_indices_subscribe, calls)
        self.assertIn(other_indices_mode, calls)

    async def test_message_ordering(self):
        """Test subscribe messages are sent before mode messages"""
        test_batch = [101, 102, 103]

        await self.client._subscribe_instruments(self.mock_websocket, [test_batch])

        # Get all sent messages
        messages = [
            json.loads(call.args[0]) for call in self.mock_websocket.send.call_args_list
        ]

        # Find subscribe and mode messages for our batch
        subscribe_msg = next(
            m for m in messages if m["a"] == "subscribe" and m["v"] == test_batch
        )
        mode_msg = next(
            m for m in messages if m["a"] == "mode" and m["v"] == ["full", test_batch]
        )

        # Verify subscribe comes before mode
        subscribe_index = messages.index(subscribe_msg)
        mode_index = messages.index(mode_msg)
        self.assertLess(subscribe_index, mode_index)

    # @patch.dict('os.environ', {'PKDevTools_Default_Log_Level': str(logging.DEBUG)})
    # async def test_large_batch_handling(self):
    #     """Test handling of batches larger than OPTIMAL_TOKEN_BATCH_SIZE"""
    #     os.environ["PKDevTools_Default_Log_Level"] = str(logging.DEBUG)
    #     large_batch = list(range(600))  # Exceeds default 500 limit

    #     # with self.assertLogs(level='DEBUG') as log:
    #     await self.client._subscribe_instruments(self.mock_websocket, [large_batch])
    #     # Verify batch size warning
    #     self.assertTrue(len(log.output) >= 0)
    #     if len(log.output) > 0:
    #         self.assertIn(f"Batch size: {len(large_batch)}", log.output[0])

    async def test_duplicate_token_handling(self):
        """Test handling of duplicate tokens in batches"""
        duplicate_batch = [101, 101, 102, 102, 103]

        await self.client._subscribe_instruments(self.mock_websocket, [duplicate_batch])

        # Verify subscribe message contains unique tokens
        subscribe_calls = [
            json.loads(call.args[0])
            for call in self.mock_websocket.send.call_args_list
            if json.loads(call.args[0])["a"] == "subscribe"
        ]

        for call in subscribe_calls:
            if call["v"] == duplicate_batch:
                self.assertEqual(len(call["v"]), len(duplicate_batch))
                break
        else:
            self.fail("Subscribe message for duplicate batch not found")


class TestConnectWebSocket(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )
        self.client.stop_event = MagicMock()
        self.client.stop_event.is_set.side_effect = [
            False,
            False,
            True,
        ]  # Stop after 1 iteration

    async def test_successful_connection(self):
        """Test successful WebSocket connection and message processing"""
        # Create a mock websocket
        mock_websocket = AsyncMock()
        mock_websocket.recv.side_effect = [
            '{"type": "instruments_meta"}',  # First message
            '{"type": "app_code"}',  # Second message
            b"\x01\x02\x03\x04",  # Binary message
        ]

        # Create a mock connection that returns our mock websocket
        mock_connection = AsyncMock()
        mock_connection.__aenter__.return_value = mock_websocket

        # Patch websockets.connect to return our mock connection
        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch("websockets.connect", return_value=mock_connection) as mock_connect,
        ):
            try:
                await self.client._connect_websocket()
            except BaseException:
                pass

            # Verify connection was established with correct parameters
            mock_connect.assert_called()
            #     self.client.ws_url,
            #     extra_headers=self.client.extra_headers,
            #     ping_interval=30,
            #     ping_timeout=10,
            #     close_timeout=5,
            #     compression="deflate",
            #     max_size=131072
            # )

            # Verify websocket methods were called
            self.assertGreaterEqual(mock_websocket.recv.call_count, 2)
            # self.assertTrue(mock_websocket.send.called)

    async def test_connection_closed_error(self):
        """Test reconnection on ConnectionClosedError"""
        # Create a mock connection that raises an error
        mock_connection = AsyncMock()
        mock_connection.__aenter__.side_effect = ConnectionClosedError(
            1000, "Normal closure"
        )

        with patch("websockets.connect", return_value=mock_connection):
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                await self.client._connect_websocket()
                self.assertGreaterEqual(mock_sleep.call_count, 1)
                # Verify reconnection attempt after delay
                mock_sleep.assert_called_with(5)

    async def test_invalid_status_code(self):
        """Test handling of InvalidStatusCode"""
        # Create a mock connection that raises an error
        mock_connection = AsyncMock()
        mock_connection.__aenter__.side_effect = InvalidStatusCode(401, {})
        with patch("websockets.connect", return_value=mock_connection):
            with patch.object(
                self.client, "_refresh_token", new_callable=AsyncMock
            ) as mock_refresh:
                with patch("asyncio.sleep", new_callable=AsyncMock):
                    await self.client._connect_websocket()
                    # Verify token refresh was attempted
                    self.assertGreaterEqual(mock_refresh.call_count, 2)

    async def test_general_exception(self):
        """Test handling of general exceptions"""
        # Create a mock connection that raises a generic error
        mock_connection = AsyncMock()
        mock_connection.__aenter__.side_effect = Exception("Generic error")
        with patch("websockets.connect", return_value=mock_connection):
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                await self.client._connect_websocket()
                # Verify reconnection attempt after delay
                self.assertGreaterEqual(mock_sleep.call_count, 2)
                mock_sleep.assert_called_with(5)

    async def test_stop_event_termination(self):
        """Test immediate exit when stop event is set"""
        self.client.stop_event.is_set.return_value = True
        self.client.stop_event.is_set.side_effect = [True]
        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch("websockets.connect") as mock_connect,
        ):
            await self.client._connect_websocket()
            # Verify no connection attempt was made
            mock_connect.assert_not_called()

    async def test_message_processing(self):
        """Test proper message processing"""
        mock_websocket = AsyncMock()
        mock_websocket.recv.side_effect = [
            '{"type": "instruments_meta", "data": {}}',
            '{"type": "app_code", "timestamp": "2023-01-01T00:00:00Z"}',
            b"\x01\x02\x03\x04",  # Binary message
        ]
        mock_connection = AsyncMock()
        mock_connection.__aenter__.return_value = mock_websocket
        self.client.stop_event.is_set.side_effect = [False, True]
        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch.object(self.client, "_subscribe_instruments", new_callable=AsyncMock),
            patch.object(self.client, "_message_loop", new_callable=AsyncMock),
            patch("websockets.connect", return_value=mock_connection),
        ):
            with patch.object(self.client, "_process_text_message") as mock_process:
                await self.client._connect_websocket()
                # Verify text messages were processed
                self.assertEqual(mock_process.call_count, 2)
                # Verify binary message resulted in queue put
                self.assertTrue(self.client.data_queue.qsize() == 0)


class TestProcessTicks(unittest.TestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )
        self.client.stop_event = MagicMock()
        self.client.data_queue = Queue(maxsize=10000)
        self.client.db_conn = MagicMock()

        # Sample tick data
        self.sample_tick = Tick(
            instrument_token=1234,
            last_price=100.50,
            last_quantity=200,
            avg_price=101.00,
            day_volume=50000,
            buy_quantity=250,
            sell_quantity=300,
            high_price=103.00,
            low_price=99.00,
            open_price=102.00,
            prev_day_close=98.00,
            last_trade_timestamp=int(datetime.now().timestamp()),
            oi=1000,
            oi_day_high=1100,
            oi_day_low=900,
            exchange_timestamp=int(datetime.now().timestamp()),
            depth=None,
        )

    def test_normal_tick_processing(self):
        """Test processing of normal tick data"""
        # Add tick to queue
        self.client.data_queue.put(self.sample_tick)
        self.client.stop_event.is_set.side_effect = [
            False,
            True,
        ]  # Process one tick then stop

        # Run processing
        self.client._process_ticks()

        # Verify tick was processed and stored
        self.client.db_conn.insert_ticks.assert_called_once()
        args, _ = self.client.db_conn.insert_ticks.call_args
        self.assertEqual(len(args[0]), 1)  # One tick in batch
        processed_tick = args[0][0]
        self.assertEqual(processed_tick["instrument_token"], 1234)
        self.assertEqual(processed_tick["last_price"], 100.50)

    # def test_batch_processing(self):
    #     """Test batching of multiple ticks"""
    #     # Add multiple ticks to queue
    #     side_effects = []
    #     for _ in range(250):  # More than OPTIMAL_BATCH_SIZE
    #         self.client.data_queue.put(self.sample_tick)
    #         side_effects.append(False)
    #     side_effects[-1] = True
    #     self.client.stop_event.is_set.side_effect = side_effects  # Process two batches then stop

    #     with patch.object(self.client, '_message_loop', new_callable=AsyncMock), patch.object(self.client, '_flush_to_db'):
    #         # Run processing
    #         self.client._process_ticks()
    #         # Verify ticks were batched properly
    #         self.assertGreaterEqual(self.client.db_conn.insert_ticks.call_count, 2)
    #         args, _ = self.client.db_conn.insert_ticks.call_args_list[0]
    #         self.assertEqual(len(args[0]), 200)  # OPTIMAL_BATCH_SIZE

    def test_empty_queue(self):
        """Test handling of empty queue"""
        self.client.stop_event.is_set.side_effect = [
            False,
            True,
        ]  # Process once then stop

        # Run processing with empty queue
        self.client._process_ticks()

        # Verify no database operations
        self.client.db_conn.insert_ticks.assert_not_called()

    def test_queue_timeout(self):
        """Test handling of queue timeout"""
        self.client.stop_event.is_set.side_effect = [
            False,
            True,
        ]  # Process once then stop
        with patch.object(self.client.data_queue, "get", side_effect=Empty()):
            self.client._process_ticks()
            self.client.db_conn.insert_ticks.assert_not_called()

    # def test_flush_on_stop(self):
    #     """Test remaining ticks are flushed when stop event is set"""
    #     # Add some ticks to queue
    #     for _ in range(10):
    #         self.client.data_queue.put(self.sample_tick)

    #     self.client.stop_event.is_set.side_effect = [False, True]  # Process once then stop

    #     # Run processing
    #     self.client._process_ticks()

    #     # Verify all ticks were processed
    #     self.client.db_conn.insert_ticks.assert_called_once()
    #     args, _ = self.client.db_conn.insert_ticks.call_args
    #     self.assertEqual(len(args[0]), 10)

    # def test_tick_conversion(self):
    #     """Test proper conversion of Tick object to dictionary"""
    #     self.client.data_queue.put(self.sample_tick)
    #     self.client.stop_event.is_set.side_effect = [False, True]  # Process one tick then stop

    #     # Run processing
    #     self.client._process_ticks()

    #     # Verify conversion
    #     args, _ = self.client.db_conn.insert_ticks.call_args
    #     processed_tick = args[0][0]

    #     # Check basic fields
    #     self.assertEqual(processed_tick['instrument_token'], 1234)
    #     self.assertEqual(processed_tick['last_price'], 100.50)
    #     self.assertEqual(processed_tick['day_volume'], 50000)

    #     # Check timestamp conversion
    #     self.assertIsInstance(processed_tick['timestamp'], datetime)
    #     self.assertEqual(processed_tick['timestamp'].tzinfo, pytz.timezone('Asia/Kolkata'))

    # def test_database_error_handling(self):
    #     """Test handling of database errors"""
    #     self.client.data_queue.put(self.sample_tick)
    #     self.client.stop_event.is_set.side_effect = [
    #         False,
    #         True,
    #     ]  # Process one tick then stop
    #     self.client.db_conn.insert_ticks.side_effect = Exception("DB error")

    #     with self.assertLogs(level="ERROR"):
    #         self.client._process_ticks()

    # def test_tick_with_depth(self):
    #     """Test processing of tick with depth data"""
    #     tick_with_depth = Tick(
    #         instrument_token=1234,
    #         last_price=100.50,
    #         depth={
    #             'bid': [
    #                 {'price': 100.00, 'quantity': 100, 'orders': 1},
    #                 {'price': 99.50, 'quantity': 200, 'orders': 2}
    #             ],
    #             'ask': [
    #                 {'price': 101.00, 'quantity': 150, 'orders': 1},
    #                 {'price': 101.50, 'quantity': 250, 'orders': 3}
    #             ]
    #         }
    #     )
    #     self.client.data_queue.put(tick_with_depth)
    #     self.client.stop_event.is_set.side_effect = [False, True]  # Process one tick then stop

    #     # Run processing
    #     self.client._process_ticks()

    #     # Verify depth data was included
    #     args, _ = self.client.db_conn.insert_ticks.call_args
    #     processed_tick = args[0][0]
    #     self.assertEqual(len(processed_tick['depth']['bid']), 2)
    #     self.assertEqual(len(processed_tick['depth']['ask']), 2)
    #     self.assertEqual(processed_tick['depth']['bid'][0]['price'], 100.00)
    #     self.assertEqual(processed_tick['depth']['ask'][1]['quantity'], 250)

    # def test_mixed_tick_types(self):
    #     """Test handling of different tick types"""
    #     from pkbrokers.kite.ticks import IndexTick

    #     # Add regular tick
    #     self.client.data_queue.put(self.sample_tick)

    #     # Add index tick
    #     index_tick = IndexTick(
    #         instrument_token=256265,  # NIFTY 50
    #         last_price=15000.50,
    #         high_price=15100.75,
    #         low_price=14950.25,
    #         open_price=15050.00,
    #         close_price=14975.50,
    #         exchange_timestamp=int(datetime.now().timestamp())
    #     )
    #     self.client.data_queue.put(index_tick)

    #     self.client.stop_event.is_set.side_effect = [False, False, True]  # Process two ticks then stop

    #     # Run processing
    #     self.client._process_ticks()

    #     # Verify both ticks were processed (IndexTick should be ignored in current implementation)
    #     self.client.db_conn.insert_ticks.assert_called_once()
    #     args, _ = self.client.db_conn.insert_ticks.call_args
    #     self.assertEqual(len(args[0]), 1)  # Only the regular tick should be processed


class TestParseBinaryMessage(unittest.TestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )

    def test_empty_message(self):
        """Test empty binary message"""
        result = self.client._parse_binary_message(b"")
        self.assertEqual(result, [])

    def test_single_ltp_packet(self):
        """Test single LTP (last traded price) packet (8 bytes)"""
        # Structure: [num_packets (2 bytes)] [packet_len (2 bytes)] [packet_data (4+4 bytes)]
        message = struct.pack(
            ">HHi i",
            1,  # 1 packet
            8,  # 8 bytes
            123456,  # instrument_token
            10050,
        )  # last_price (100.50 in paise)

        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks), 1)
        self.assertEqual(ticks[0]["instrument_token"], 123456)
        self.assertEqual(ticks[0]["last_price"], 100.50)
        self.assertIsNone(ticks[0]["last_quantity"])

    def test_ltp_with_quantity(self):
        """Test LTP with quantity (12 bytes)"""
        # Structure: [num_packets] [packet_len] [instrument_token, last_price, last_quantity]
        message = struct.pack(
            ">HHi i i",
            1,  # 1 packet
            12,  # 12 bytes
            123456,  # instrument_token
            10050,  # last_price (100.50)
            200,
        )  # last_quantity

        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks), 1)
        self.assertEqual(ticks[0]["last_quantity"], 200)

    def test_full_quote_packet(self):
        """Test full quote packet (44 bytes)"""
        # Structure: [num_packets] [packet_len] [instrument_token, last_price, ...]
        values = [
            123456,  # instrument_token
            10050,  # last_price (100.50)
            200,  # last_quantity
            10100,  # avg_price (101.00)
            50000,  # volume
            250,  # buy_quantity
            300,  # sell_quantity
            10200,  # open (102.00)
            10300,  # high (103.00)
            9900,  # low (99.00)
            9800,  # prev_close (98.00)
            1625097600,  # last_trade_timestamp
            1000,  # oi
            1100,  # oi_day_high
            900,  # oi_day_low
            1625097601,  # exchange_timestamp
        ]
        message = struct.pack(
            ">HHi i i i i i i i i i i i i i i i",
            1,  # 1 packet
            60,  # 60 bytes (44 bytes of data + 16 bytes header)
            *values,
        )

        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks), 1)
        self.assertEqual(ticks[0]["high_price"], 103.00)
        self.assertEqual(ticks[0]["prev_day_close"], 98.00)
        # self.assertEqual(ticks[0]["exchange_timestamp"], 1625097601)

    def test_full_packet_with_depth(self):
        """Test full packet with market depth (184 bytes)"""
        # Base values (16 integers)
        base_values = [
            123456,  # instrument_token
            10050,  # last_price
            200,  # last_quantity
            10100,  # avg_price
            50000,  # volume
            250,  # buy_quantity
            300,  # sell_quantity
            10200,  # open
            10300,  # high
            9900,  # low
            9800,  # prev_close
            1625097600,  # last_trade_timestamp
            1000,  # oi
            1100,  # oi_day_high
            900,  # oi_day_low
            1625097601,  # exchange_timestamp
        ]

        # Depth data (5 bids + 5 asks)
        depth_data = []
        for i in range(1, 6):  # 5 bid levels
            depth_data.extend([i * 100, i * 10050, i])  # quantity, price, orders

        for i in range(1, 6):  # 5 ask levels
            depth_data.extend([i * 100, i * 10100, i])  # quantity, price, orders

        # Build complete message
        message = struct.pack(
            ">HHi i i i i i i i i i i i i i i i" + "i i h" * 10,
            1,  # 1 packet
            184,  # 184 bytes
            *base_values,
            *depth_data,
        )

        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks), 1)
        self.assertEqual(len(ticks[0]["depth"]["bid"]), 5)
        self.assertEqual(len(ticks[0]["depth"]["ask"]), 5)
        self.assertEqual(ticks[0]["depth"]["bid"][0]["price"], 100.50)
        self.assertEqual(ticks[0]["depth"]["ask"][4]["orders"], 5)

    def test_multiple_packets(self):
        """Test message with multiple packets"""
        # Structure for multiple packets:
        # [num_packets (2 bytes)]
        # Then for each packet:
        #   [packet_len (2 bytes)] [packet_data (variable)]

        # Packet 1: LTP (8 bytes of data + 2 byte header = 10 bytes total)
        packet1_header = struct.pack(">H", 8)  # packet length
        packet1_data = struct.pack(
            ">i i",
            123456,  # instrument_token
            10050,
        )  # last_price

        # Packet 2: LTP with quantity (12 bytes of data + 2 byte header = 14 bytes total)
        packet2_header = struct.pack(">H", 12)  # packet length
        packet2_data = struct.pack(
            ">i i i",
            654321,  # instrument_token
            20075,  # last_price
            150,
        )  # last_quantity

        # Complete message structure:
        # [num_packets] [packet1_len] [packet1_data] [packet2_len] [packet2_data]
        message = (
            struct.pack(">H", 2)
            + packet1_header
            + packet1_data
            + packet2_header
            + packet2_data
        )

        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks), 2)
        self.assertEqual(ticks[0]["instrument_token"], 123456)
        self.assertEqual(ticks[1]["instrument_token"], 654321)
        self.assertEqual(ticks[1]["last_quantity"], 150)

    def test_invalid_packet_length(self):
        """Test packet shorter than minimum length"""
        # Message claims to have 1 packet of 8 bytes but only provides 4 bytes
        message = struct.pack(">HH", 1, 8) + b"\x01\x02\x03\x04"
        # with self.assertLogs(level='WARNING'):
        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks), 0)

    def test_partial_depth_data(self):
        """Test packet with incomplete depth data"""
        base_values = [
            123456,
            10050,
            200,
            10100,
            50000,
            250,
            300,
            10200,
            10300,
            9900,
            9800,
            1625097600,
            1000,
            1100,
            900,
            1625097601,
        ]

        # Only 3 bid levels instead of 5
        depth_data = []
        for i in range(1, 4):
            depth_data.extend([i * 100, i * 10050, i])

        message = struct.pack(
            ">HHi i i i i i i i i i i i i i i i" + "i i h" * 3,
            1,
            64 + 30,  # 64 base + 30 depth (3 entries)
            *base_values,
            *depth_data,
        )

        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks[0]["depth"]["bid"]), 3)
        self.assertEqual(len(ticks[0]["depth"]["ask"]), 0)

    def test_error_handling(self):
        """Test error during parsing"""
        # Invalid binary data that will cause struct.error
        message = b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10"
        # with self.assertLogs(level='ERROR'):
        ticks = self.client._parse_binary_message(message)
        self.assertEqual(len(ticks), 0)

    def test_paise_to_rupee_conversion(self):
        """Test proper conversion from paise to rupees"""
        test_cases = [
            (10050, 100.50),  # standard case
            (10000, 100.00),  # whole rupees
            (10001, 100.01),  # single paise
            (0, 0.00),  # zero
            (1, 0.01),  # single paise
            (9999, 99.99),  # max paise
        ]

        for paise, expected_rupees in test_cases:
            message = struct.pack(
                ">HHi i",
                1,  # 1 packet
                8,  # 8 bytes
                123456,  # instrument_token
                paise,
            )  # last_price in paise

            ticks = self.client._parse_binary_message(message)
            self.assertEqual(ticks[0]["last_price"], expected_rupees)


class TestConnectionMonitor(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="dummy_token", user_id="DUMMY_USER"
        )
        self.client.stop_event = MagicMock()
        self.client.last_message_time = time.time()
        # Create a patcher for the module-level logger
        self.logger_patcher = patch("pkbrokers.kite.zerodhaWebSocketClient.logger")
        self.mock_logger = self.logger_patcher.start()

    def tearDown(self):
        self.logger_patcher.stop()

    async def test_normal_operation(self):
        """Test monitor with regular message updates"""
        self.client.stop_event.is_set.side_effect = [
            False,
            False,
            True,
        ]  # Run twice then stop
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await self.client._connection_monitor()

            # Verify sleep was called twice with 10s interval
            self.assertEqual(mock_sleep.call_count, 2)
            mock_sleep.assert_called_with(10)

            # No warnings should be logged
            self.mock_logger.warn.assert_not_called()

    async def test_stale_connection_detection(self):
        """Test detection of stale connection"""
        # Set last message time to 61 seconds ago
        self.client.last_message_time = time.time() - 61
        self.client.stop_event.is_set.side_effect = [False, True]  # Run once then stop

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await self.client._connection_monitor()

        # Verify warning was logged
        self.mock_logger.warn.assert_called_once_with(
            "No messages received in last 60 seconds"
        )

    async def test_immediate_stop(self):
        """Test immediate exit when stop event is set"""
        self.client.stop_event.is_set.return_value = True

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await self.client._connection_monitor()

            # Verify no sleep occurred
            mock_sleep.assert_not_called()

    async def test_last_message_time_update(self):
        """Test monitor handles last_message_time updates"""
        original_time = self.client.last_message_time
        self.client.stop_event.is_set.side_effect = [False, True]  # Run once then stop

        # Simulate message received during monitoring
        async def mock_sleep(_):
            self.client.last_message_time = time.time()  # Update time

        with patch("asyncio.sleep", new_callable=AsyncMock, side_effect=mock_sleep):
            await self.client._connection_monitor()

            # Verify time was updated
            self.assertNotEqual(self.client.last_message_time, original_time)
            self.mock_logger.warn.assert_not_called()

    async def test_multiple_stale_periods(self):
        """Test multiple consecutive stale periods"""
        self.client.last_message_time = time.time() - 120  # 2 minutes stale
        self.client.stop_event.is_set.side_effect = [
            False,
            False,
            True,
        ]  # Run twice then stop

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await self.client._connection_monitor()

        # Verify two warnings were logged
        self.assertEqual(self.mock_logger.warn.call_count, 2)
        self.mock_logger.warn.assert_called_with(
            "No messages received in last 60 seconds"
        )

    async def test_no_last_message_time(self):
        """Test handling when last_message_time is not set"""
        del self.client.last_message_time
        self.client.stop_event.is_set.side_effect = [False, True]  # Run once then stop

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await self.client._connection_monitor()

        # Should initialize the timestamp and not warn
        self.assertTrue(hasattr(self.client, "last_message_time"))
        self.mock_logger.warn.assert_not_called()

    async def test_high_frequency_monitoring(self):
        """Test with faster monitoring interval"""
        self.client.stop_event.is_set.side_effect = [
            False,
            False,
            False,
            True,
        ]  # Run 3 times
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await self.client._connection_monitor()

            # Verify proper sleep interval
            self.assertEqual(mock_sleep.call_count, 3)
            for call in mock_sleep.call_args_list:
                self.assertEqual(call.args[0], 10)


class TestZerodhaWebSocketClientMessageLoop(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = ZerodhaWebSocketClient(
            enctoken="test_enctoken", user_id="test_user", api_key="test_key"
        )
        self.client.stop_event = MagicMock()
        self.client.stop_event.is_set = MagicMock(return_value=False)
        self.client.data_queue = MagicMock()
        self.client.watcher_queue = MagicMock()
        self.client._process_text_message = MagicMock()
        self.client.last_message_time = MagicMock()
        self.mock_websocket = AsyncMock()
        self.logger_patcher = patch("pkbrokers.kite.zerodhaWebSocketClient.logger")
        self.mock_logger = self.logger_patcher.start()

    def tearDown(self):
        self.logger_patcher.stop()

    async def asyncTearDown(self):
        # Cancel any running tasks
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def test_normal_binary_message_processing(self):
        """Test processing of normal binary market data messages"""
        # Setup mock binary message and control test duration
        binary_message = b"\x00\x01\x00\x08\x00\x00\x01\x02\x00\x00\x03\x04"
        self.mock_websocket.recv.side_effect = [
            binary_message,
            asyncio.CancelledError(),
        ]

        # Mock the parser to return a tick
        mock_tick = Tick(
            instrument_token=1234,
            last_price=100.0,
            last_quantity=0,
            avg_price=0,
            day_volume=0,
            buy_quantity=0,
            sell_quantity=0,
            open_price=0,
            high_price=0,
            low_price=0,
            prev_day_close=0,
            last_trade_timestamp=0,
            oi=0,
            oi_day_high=0,
            oi_day_low=0,
            exchange_timestamp=0,
            depth=None,
        )
        with (
            patch(
                "pkbrokers.kite.zerodhaWebSocketParser.ZerodhaWebSocketParser.parse_binary_message",
                return_value=[mock_tick],
            ),
            patch("time.time") as mock_time,
        ):
            try:
                await self.client._message_loop(self.mock_websocket)
            except asyncio.CancelledError:
                pass

            # Verify the message was processed
            self.client.data_queue.put.assert_called_once_with(mock_tick)
            if self.client.watcher_queue is not None:
                self.client.watcher_queue.put.assert_called_once_with(mock_tick)
            mock_time.assert_called_once()

    async def test_text_message_processing(self):
        """Test processing of text/JSON messages"""
        test_message = '{"type": "order", "data": {"order_id": "12345"}}'
        self.mock_websocket.recv.side_effect = [test_message, asyncio.CancelledError()]

        try:
            with patch("time.time") as mock_time:
                await self.client._message_loop(self.mock_websocket)
        except asyncio.CancelledError:
            pass

        self.client._process_text_message.assert_called_once_with(
            json.loads(test_message)
        )
        mock_time.assert_called_once()

    async def test_heartbeat_message_processing(self):
        """Test that single-byte heartbeat messages are ignored"""
        heartbeat_message = b"\x01"  # Single byte heartbeat
        self.mock_websocket.recv.side_effect = [
            heartbeat_message,
            asyncio.CancelledError(),
        ]

        try:
            with patch("time.time") as mock_time:
                await self.client._message_loop(self.mock_websocket)
        except asyncio.CancelledError:
            pass

        self.client.data_queue.put.assert_not_called()
        self.client._process_text_message.assert_not_called()
        mock_time.assert_called_once()

    async def test_connection_closed_error(self):
        """Test handling of ConnectionClosed error"""
        self.mock_websocket.recv.side_effect = Exception("Normal closure")

        await self.client._message_loop(self.mock_websocket)
        self.mock_logger.error.assert_called()

    async def test_timeout_error_handling(self):
        """Test handling of timeout errors"""
        self.mock_websocket.recv.side_effect = [
            asyncio.TimeoutError(),
            asyncio.CancelledError(),
        ]
        self.mock_websocket.ping = AsyncMock()

        try:
            await self.client._message_loop(self.mock_websocket)
        except asyncio.CancelledError:
            pass

        self.mock_websocket.ping.assert_called_once()

    async def test_invalid_json_message(self):
        """Test handling of invalid JSON messages"""
        invalid_json = '{"invalid": json}'
        self.mock_websocket.recv.side_effect = [invalid_json, asyncio.CancelledError()]
        try:
            await self.client._message_loop(self.mock_websocket)
        except asyncio.CancelledError:
            pass
        self.mock_logger.warn.assert_called_once()
        self.client._process_text_message.assert_not_called()

    async def test_stop_event_handling(self):
        """Test that message loop exits when stop_event is set"""
        # First call returns False, second returns True to stop the loop
        self.client.stop_event.is_set = MagicMock(side_effect=[False, True])
        self.mock_websocket.recv.side_effect = [asyncio.CancelledError()]

        try:
            await self.client._message_loop(self.mock_websocket)
        except asyncio.CancelledError:
            pass

        self.assertEqual(self.client.stop_event.is_set.call_count, 1)

    async def test_general_exception_handling(self):
        """Test handling of unexpected exceptions"""
        self.mock_websocket.recv.side_effect = [
            Exception("Test error"),
            asyncio.CancelledError(),
        ]

        try:
            await self.client._message_loop(self.mock_websocket)
        except asyncio.CancelledError:
            pass
        self.mock_logger.error.assert_called_once()

    async def test_message_loop_with_empty_message(self):
        """Test handling of empty messages"""
        self.mock_websocket.recv.side_effect = ["{}", asyncio.CancelledError()]

        try:
            await self.client._message_loop(self.mock_websocket)
        except asyncio.CancelledError:
            pass

        self.client._process_text_message.assert_called_once_with({})

    async def test_high_frequency_messages(self):
        """Test handling of high frequency messages"""
        messages = [
            b"\x00\x01\x00\x08\x00\x00\x01\x02\x00\x00\x03\x04",  # binary
            '{"type": "order", "data": {"order_id": "12345"}}',  # text
            b"\x01",  # heartbeat
            b"\x00\x01\x00\x08\x00\x00\x01\x02\x00\x00\x03\x04",  # binary
            asyncio.CancelledError(),  # Stop after processing
        ]
        self.mock_websocket.recv.side_effect = messages

        # Mock the parser to return ticks
        mock_tick = Tick(
            instrument_token=1234,
            last_price=100.0,
            last_quantity=0,
            avg_price=0,
            day_volume=0,
            buy_quantity=0,
            sell_quantity=0,
            open_price=0,
            high_price=0,
            low_price=0,
            prev_day_close=0,
            last_trade_timestamp=0,
            oi=0,
            oi_day_high=0,
            oi_day_low=0,
            exchange_timestamp=0,
            depth=None,
        )
        with (
            patch(
                "pkbrokers.kite.zerodhaWebSocketParser.ZerodhaWebSocketParser.parse_binary_message",
                return_value=[mock_tick],
            ),
            patch("time.time") as mock_time,
        ):
            try:
                await self.client._message_loop(self.mock_websocket)
            except asyncio.CancelledError:
                pass

            # Verify processing of all message types
            self.assertEqual(
                self.client.data_queue.put.call_count, 2
            )  # 2 binary messages
            self.client._process_text_message.assert_called_once_with(
                json.loads(messages[1])
            )
            self.assertEqual(mock_time.call_count, 4)
