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

import asyncio
import base64
import json
import os
import queue
import sqlite3
import struct
import threading
import time
from datetime import datetime
from queue import Queue
from urllib.parse import quote

import dateutil
import pytz
import websockets
from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger

from pkbrokers.kite.threadSafeDatabase import ThreadSafeDatabase
from pkbrokers.kite.tickMonitor import TickMonitor
from pkbrokers.kite.ticks import IndexTick, Tick
from pkbrokers.kite.zerodhaWebSocketParser import ZerodhaWebSocketParser

DEFAULT_PATH = Archiver.get_user_data_dir()

PING_INTERVAL = 30
OPTIMAL_BATCH_SIZE = 200  # Adjust based on testing
OPTIMAL_TOKEN_BATCH_SIZE = 500  # Zerodha allows max 500 instruments in one batch
NIFTY_50 = [256265]
BSE_SENSEX = [265]
OTHER_INDICES = [
    264969,
    263433,
    260105,
    257545,
    261641,
    262921,
    257801,
    261897,
    261385,
    259849,
    263945,
    263689,
    262409,
    261129,
    263177,
    260873,
    256777,
    266249,
    289545,
    274185,
    274441,
    275977,
    278793,
    279305,
    291593,
    289801,
    281353,
    281865,
]


class ZerodhaWebSocketClient:
    def __init__(
        self,
        enctoken,
        user_id,
        api_key="kitefront",
        token_batches=[],
        watcher_queue=None,
    ):
        self.watcher_queue = watcher_queue
        self.enctoken = enctoken
        self.user_id = user_id
        self.api_key = api_key
        # Configure logging
        self.logger = default_logger()
        self.ws_url = self._build_websocket_url()
        self.data_queue = Queue(maxsize=10000)
        self.stop_event = threading.Event()
        self.db_conn = ThreadSafeDatabase()
        self.extra_headers = self._build_headers()
        self.last_message_time = time.time()
        self.last_heartbeat = time.time()
        self.token_batches = token_batches
        self.token_timestamp = 0
        self.ws_tasks = []
        self.index_subscribed = True

    def _build_websocket_url(self):
        """Construct the WebSocket URL with proper parameters"""
        if self.api_key is None or len(self.api_key) == 0:
            raise ValueError("API Key must not be blank")
        if self.user_id is None or len(self.user_id) == 0:
            raise ValueError("user_id must not be blank")
        if self.enctoken is None or len(self.enctoken) == 0:
            raise ValueError("enctoken must not be blank")
        base_params = {
            "api_key": self.api_key,
            "user_id": self.user_id,
            "enctoken": quote(self.enctoken),
            "uid": str(int(time.time() * 1000)),
            "user-agent": "kite3-web",
            "version": "3.0.0",
        }
        query_string = "&".join([f"{k}={v}" for k, v in base_params.items()])
        return f"wss://ws.zerodha.com/?{query_string}"

    def _build_headers(self):
        """Generate required WebSocket headers"""
        # Generate random WebSocket key (required for handshake)
        ws_key = base64.b64encode(os.urandom(16)).decode("utf-8")

        return {
            "Host": "ws.zerodha.com",
            "Connection": "Upgrade",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Upgrade": "websocket",
            "Origin": "https://kite.zerodha.com",
            "Sec-WebSocket-Version": "13",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-WebSocket-Key": ws_key,
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
        }

    def _build_tokens(self):
        import os

        from dotenv import dotenv_values

        from pkbrokers.kite.instruments import KiteInstruments

        API_KEY = "kitefront"
        ACCESS_TOKEN = ""
        try:
            local_secrets = dotenv_values(".env.dev")
            ACCESS_TOKEN = os.environ.get(
                "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
            )
        except BaseException:
            raise ValueError(
                ".env.dev file missing in the project root folder or values not set.\nYou need your Kite token."
            )
        self.enctoken = ACCESS_TOKEN
        kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)
        equities_count = kite.get_instrument_count()
        if equities_count == 0:
            kite.sync_instruments(force_fetch=True)
        equities = kite.get_equities(column_names="instrument_token")
        tokens = kite.get_instrument_tokens(equities=equities)
        self.token_batches = [
            tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
            for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
        ]

    async def _subscribe_instruments(
        self, websocket, token_batches, subscribe_all_indices=False
    ):
        """Subscribe to instruments with rate limiting"""
        if self.stop_event.is_set():
            return

        if not self.index_subscribed:
            self.index_subscribed = True
            # Subscribe to Nifty 50 index
            self.logger.debug("Sending NIFTY_50 subscribe and mode messages")
            await websocket.send(json.dumps({"a": "subscribe", "v": NIFTY_50}))
            await websocket.send(json.dumps({"a": "mode", "v": ["full", NIFTY_50]}))

            # Subscribe to BSE Sensex
            self.logger.debug("Sending BSE_SENSEX subscribe and mode messages")
            await websocket.send(json.dumps({"a": "subscribe", "v": BSE_SENSEX}))
            await websocket.send(json.dumps({"a": "mode", "v": ["full", BSE_SENSEX]}))

            if subscribe_all_indices:
                self.logger.debug("Sending OTHER_INDICES subscribe and mode messages")
                await websocket.send(json.dumps({"a": "subscribe", "v": OTHER_INDICES}))
                await websocket.send(
                    json.dumps({"a": "mode", "v": ["full", OTHER_INDICES]})
                )

        for batch in token_batches:
            if self.stop_event.is_set():
                break

            subscribe_msg = {"a": "subscribe", "v": batch}
            # There are three different modes in which quote packets are streamed.
            # modes:
            # ltp	    LTP. Packet contains only the last traded price (8 bytes).
            # ltpc	    LTPC. Packet contains only the last traded price and close price (16 bytes).
            # quote	    Quote. Packet contains several fields excluding market depth (44 bytes).
            # full	    Full. Packet contains several fields including market depth (184 bytes).

            mode_msg = {"a": "mode", "v": ["full", batch]}
            self.logger.debug(
                f"Batch size: {len(batch)}. Sending subscribe message: {subscribe_msg}"
            )
            await websocket.send(json.dumps(subscribe_msg))

            self.logger.debug(f"Sending mode message: {mode_msg}")
            await websocket.send(json.dumps(mode_msg))

            await asyncio.sleep(1)  # Respect rate limits

    async def send_heartbeat(self, websocket):
        # Send heartbeat every 30 seconds
        if time.time() - self.last_heartbeat > PING_INTERVAL:
            await websocket.send(json.dumps({"a": "ping"}))
            self.last_heartbeat = time.time()

    async def _connect_websocket(self, token_batch=[]):
        """Establish and maintain WebSocket connection"""

        while not self.stop_event.is_set():
            try:
                async with (
                    websockets.connect(
                        self._build_websocket_url(),
                        extra_headers=self._build_headers(),
                        ping_interval=PING_INTERVAL,
                        ping_timeout=10,
                        close_timeout=5,
                        compression="deflate",  # Disable compression for debugging (None instead of deflate)
                        max_size=2**17,  # 128KB max message size
                    ) as websocket
                ):
                    self.logger.debug("WebSocket connected successfully")

                    # Wait for initial messages
                    initial_messages = []
                    max_wait_counter = 2
                    wait_counter = 0
                    while len(initial_messages) < 2 and wait_counter < max_wait_counter:
                        wait_counter += 1
                        message = await websocket.recv()
                        if isinstance(message, str):
                            data = json.loads(message)
                            if data.get("type") in ["instruments_meta", "app_code"]:
                                initial_messages.append(data)
                                self.logger.debug(f"Received initial message: {data}")
                                self._process_text_message(data=data)
                        await asyncio.sleep(1)
                    # Subscribe to instruments (example tokens)
                    if len(self.token_batches) == 0:
                        self._build_tokens()
                    await self._subscribe_instruments(
                        websocket,
                        self.token_batches if len(token_batch) == 0 else token_batch,
                    )

                    # Heartbeat every 30 seconds
                    self.last_heartbeat = time.time()

                    # Main message loop
                    await self._message_loop(websocket)

            except websockets.exceptions.ConnectionClosedError as e:
                if hasattr(e, "code"):
                    self.logger.error(f"Connection closed: {e.code} - {e.reason}")
                    if e.code == 1000:
                        self.logger.debug("Normal closure, reconnecting...")
                    elif e.code == 1011:
                        self.logger.warn(
                            "(unexpected error) keepalive ping timeout, reconnecting..."
                        )
                await asyncio.sleep(5)
            except websockets.exceptions.InvalidStatusCode as e:
                if hasattr(e, "status_code"):
                    self.logger.error(f"Connection failed with status {e.status_code}")
                    if e.status_code == 400:
                        self.logger.error("Authentication failed. Please check your:")
                        self.logger.error("- API Key")
                        self.logger.error("- Access Token")
                        self.logger.error("- User ID")
                        self.logger.error("- Token expiration (tokens expire daily)")
                        self.stop()
                        return
                    elif e.status_code in [401, 403]:
                        # the token must have expired
                        self._refresh_token()

                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(
                    f"WebSocket connection error: {str(e)}. Reconnecting in 5 seconds..."
                )
                await asyncio.sleep(5)

    async def _message_loop(self, websocket):
        """Handle incoming messages according to Zerodha's spec"""
        while not self.stop_event.is_set():
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=10)
                self.last_message_time = time.time()

                if isinstance(message, bytes):
                    # Handle binary messages (market data or heartbeat)
                    if len(message) == 1:
                        # Single byte is a heartbeat, ignore
                        self.logger.debug("Heartbeat.")
                        continue
                    else:
                        self.logger.debug("Receiving Market Data.")
                        # Process market data
                        ticks = ZerodhaWebSocketParser.parse_binary_message(message)
                        for tick in ticks:
                            self.data_queue.put(tick)
                            if self.watcher_queue is not None:
                                self.watcher_queue.put(tick)
                elif isinstance(message, str):
                    self.logger.debug("Receiving Postbacks or other updates.")
                    # Handle text messages (postbacks and updates)
                    try:
                        data = json.loads(message)
                        self._process_text_message(data)
                    except json.JSONDecodeError:
                        self.logger.warn(f"Invalid JSON message: {message}")

            except asyncio.TimeoutError:
                await websocket.ping()
            except websockets.exceptions.ConnectionClosedError as e:
                if hasattr(e, "code"):
                    self.logger.error(f"Connection closed: {e.code} - {e.reason}")
                break
            except Exception as e:
                self.logger.error(f"Message processing error: {str(e)}")
                break

    def _parse_binary_message(self, message):
        """Parse binary market data messages"""
        ticks = []

        try:
            # Read number of packets (first 2 bytes)
            num_packets = struct.unpack_from(">H", message, 0)[0]
            offset = 2

            for _ in range(num_packets):
                # Read packet length (next 2 bytes)
                packet_length = struct.unpack_from(">H", message, offset)[0]
                offset += 2

                # Extract packet data
                packet = message[offset : offset + packet_length]
                offset += packet_length

                # Parse individual packet
                tick = self._parse_binary_packet(packet)
                if tick:
                    ticks.append(tick)

        except Exception as e:
            self.logger.error(f"Error parsing binary message: {str(e)}")

        return ticks

    def _parse_binary_packet(self, packet):
        """Parse individual binary packet with variable length"""
        try:
            # Minimum packet is 8 bytes (instrument_token + ltp)
            if len(packet) < 8:
                self.logger.warn(f"Packet too short: {len(packet)} bytes")
                return None

            # Unpack common fields (first 8 bytes)
            instrument_token, last_price = struct.unpack(">ii", packet[:8])
            last_price = last_price / 100  # Convert from paise to rupees

            # Initialize tick data with default values
            tick_data = {
                "instrument_token": instrument_token,
                "last_price": last_price,
                "last_quantity": None,
                "avg_price": None,
                "day_volume": None,
                "buy_quantity": None,
                "sell_quantity": None,
                "open_price": None,
                "high_price": None,
                "low_price": None,
                "prev_day_close": None,
                "last_trade_timestamp": None,
                "oi": None,
                "oi_day_high": None,
                "oi_day_low": None,
                "exchange_timestamp": None,
                "depth": None,
            }

            # Parse remaining fields based on packet length
            offset = 8

            # LTP mode (8-12 bytes)
            if len(packet) >= 12:
                last_quantity = struct.unpack_from(">i", packet, offset)[0]
                tick_data["last_quantity"] = last_quantity
                offset += 4

            # Full mode fields (12+ bytes)
            if len(packet) >= 16:
                avg_price = struct.unpack_from(">i", packet, offset)[0]
                tick_data["avg_price"] = avg_price / 100
                offset += 4

            if len(packet) >= 20:
                volume = struct.unpack_from(">i", packet, offset)[0]
                tick_data["day_volume"] = volume
                offset += 4

            if len(packet) >= 24:
                buy_quantity = struct.unpack_from(">i", packet, offset)[0]
                tick_data["buy_quantity"] = buy_quantity
                offset += 4

            if len(packet) >= 28:
                sell_quantity = struct.unpack_from(">i", packet, offset)[0]
                tick_data["sell_quantity"] = sell_quantity
                offset += 4

            if len(packet) >= 32:
                open_price = struct.unpack_from(">i", packet, offset)[0]
                tick_data["open_price"] = open_price / 100
                offset += 4

            if len(packet) >= 36:
                high_price = struct.unpack_from(">i", packet, offset)[0]
                tick_data["high_price"] = high_price / 100
                offset += 4

            if len(packet) >= 40:
                low_price = struct.unpack_from(">i", packet, offset)[0]
                tick_data["low_price"] = low_price / 100
                offset += 4

            # Quote mode ends here (40-44 bytes)
            if len(packet) >= 44:
                prev_day_close = struct.unpack_from(">i", packet, offset)[0]
                tick_data["prev_day_close"] = prev_day_close / 100
                offset += 4

            if len(packet) >= 48:
                last_trade_timestamp = struct.unpack_from(">i", packet, offset)[0]
                tick_data["last_trade_timestamp"] = last_trade_timestamp
                offset += 4

            if len(packet) >= 52:
                oi = struct.unpack_from(">i", packet, offset)[0]
                tick_data["oi"] = oi
                offset += 4

            if len(packet) >= 56:
                oi_day_high = struct.unpack_from(">i", packet, offset)[0]
                tick_data["oi_day_high"] = oi_day_high
                offset += 4

            if len(packet) >= 60:
                oi_day_low = struct.unpack_from(">i", packet, offset)[0]
                tick_data["oi_day_low"] = oi_day_low
                offset += 4

            if len(packet) >= 64:
                exchange_timestamp = struct.unpack_from(">i", packet, offset)[0]
                tick_data["exchange_timestamp"] = exchange_timestamp
                offset += 4

            # Market depth (64-184 bytes)
            if len(packet) >= 74:
                depth = {"bid": [], "ask": []}

                # Parse 5 bid entries (64-124)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from(
                            ">iih", packet, offset
                        )
                        depth["bid"].append(
                            {
                                "quantity": quantity,
                                "price": price / 100,
                                "orders": orders,
                            }
                        )
                        offset += 10
                    else:
                        break

                # Parse 5 ask entries (124-164)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from(
                            ">iih", packet, offset
                        )
                        depth["ask"].append(
                            {
                                "quantity": quantity,
                                "price": price / 100,
                                "orders": orders,
                            }
                        )
                        offset += 10
                    else:
                        break

                tick_data["depth"] = depth

            return tick_data

        except Exception as e:
            self.logger.error(f"Error parsing packet: {str(e)}")
            return None

    def _process_text_message(self, data):
        """Process non-binary JSON messages"""
        if not isinstance(data, dict):
            return

        message_type = data.get("type")

        if message_type == "order":
            self._process_order(data.get("data", {}))
        elif message_type == "error":
            self.logger.error(f"Server error: {data.get('data')}")
        elif message_type == "message":
            self.logger.debug(f"Server message: {data.get('data')}")
        elif message_type == "instruments_meta":
            # We don't use it. So we can safely ignore.
            # count
            # Represents the total number of instruments available in the market.
            # Example: "count": 86481 means there are 86,481 instruments in the current dataset.
            # This helps clients verify whether they have the complete list of instruments.
            # 2. eTag (Entity Tag)
            # Acts as a version identifier for the instrument metadata.
            # Example: "etag": "W/\"68907d60-55bf\"" is a weak ETag (indicated by W/) used for caching and change detection.
            # Purpose:
            # Clients can compare the eTag with a previously stored value to check if the instrument list has been updated.
            # If the eTag changes, it means the instrument metadata has been modified (e.g., new listings, delistings, or changes in instrument details).
            self.logger.debug(f"Instruments metadata update: {data.get('data')}")
        elif message_type == "app_code":
            self.logger.debug(f"App code update: {data}")
            self.token_timestamp = dateutil.parser.isoparse(
                data.get("timestamp", datetime.now().isoformat())
            )
            self._refresh_token()
        else:
            self.logger.debug(f"Unknown message type: {data}")

    def _process_order(self, order_data):
        """Process order updates"""
        self.logger.debug(f"Order update: {order_data}")
        # Add your order processing logic here

    def _process_ticks(self):
        """Process ticks from queue and store in database"""
        batch = []
        last_flush = time.time()

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                tick = self.data_queue.get(timeout=1)

                if tick is None:
                    continue

                # Process the tick based on its type
                if isinstance(tick, Tick):
                    # Convert to optimized format
                    processed = {
                        "instrument_token": tick.instrument_token,
                        "timestamp": datetime.fromtimestamp(
                            tick.exchange_timestamp, tz=pytz.timezone("Asia/Kolkata")
                        ),  # Explicit IST
                        "last_price": tick.last_price
                        if tick.last_price is not None
                        else 0,
                        "day_volume": tick.day_volume
                        if tick.day_volume is not None
                        else 0,
                        "oi": tick.oi if tick.oi is not None else 0,
                        "buy_quantity": tick.buy_quantity
                        if tick.buy_quantity is not None
                        else 0,
                        "sell_quantity": tick.sell_quantity
                        if tick.sell_quantity is not None
                        else 0,
                        "high_price": tick.high_price
                        if tick.high_price is not None
                        else 0,
                        "low_price": tick.low_price
                        if tick.low_price is not None
                        else 0,
                        "open_price": tick.open_price
                        if tick.open_price is not None
                        else 0,
                        "prev_day_close": tick.prev_day_close
                        if tick.prev_day_close is not None
                        else 0,
                    }
                    self.logger.debug(processed)
                    # Add depth if available
                    if tick.depth:
                        processed["depth"] = {
                            "bid": [
                                {
                                    "price": b.price,
                                    "quantity": b.quantity,
                                    "orders": b.orders,
                                }
                                for b in tick.depth["bid"][:5]  # Only first 5 levels
                            ],
                            "ask": [
                                {
                                    "price": a.price,
                                    "quantity": a.quantity,
                                    "orders": a.orders,
                                }
                                for a in tick.depth["ask"][:5]
                            ],
                        }
                    batch.append(processed)

                elif isinstance(tick, IndexTick):
                    # Handle index ticks differently if needed
                    pass

                # Flush batch if size limit reached or time elapsed
                if len(batch) >= OPTIMAL_BATCH_SIZE or (time.time() - last_flush) > 5:
                    self._flush_to_db(batch)
                    batch = []
                    last_flush = time.time()

                self.data_queue.task_done()

            except queue.Empty:
                # Flush any remaining ticks
                if batch:
                    self._flush_to_db(batch)
                    batch = []
                    last_flush = time.time()
                continue
            except Exception as e:
                self.logger.error(f"Error processing ticks: {str(e)}")

        # Flush any remaining ticks
        if batch:
            self._flush_to_db(batch)

    def _refresh_token(self, force=False):
        """Refresh expired access token"""
        if force or (time.time() - self.token_timestamp > 86400):  # 24 hours
            self.logger.debug("Refreshing access token")
            # Implement your token refresh logic here
            from pkbrokers.kite.authenticator import KiteAuthenticator

            auth = KiteAuthenticator()
            encToken = auth.get_enctoken()
            self.enctoken = encToken
            self.ws_url = self._build_websocket_url()

    async def _connection_monitor(self):
        """Monitor connection health"""
        while not self.stop_event.is_set():
            if not hasattr(self, "last_message_time"):
                self.last_message_time = time.time()

            if time.time() - self.last_message_time > 60:
                self.logger.warn("No messages received in last 60 seconds")
            await asyncio.sleep(10)

    async def _monitor_performance(self):
        """Monitor system performance"""
        conn = sqlite3.connect(os.path.join(DEFAULT_PATH, "ticks.db"), timeout=30)
        while not self.stop_event.is_set():
            # Track processing rate
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM ticks
                WHERE timestamp >= datetime('now', '-1 minute')
            """)
            ticks_per_minute = cursor.fetchone()[0]

            self.logger.debug(
                f"Performance | Queue: {self.data_queue.qsize()} | "
                f"Ticks/min: {ticks_per_minute} | "
                f"DB Lag: {self.data_queue.qsize() / max(1, ticks_per_minute / 60):.1f}s"
            )

            await asyncio.sleep(60)

    async def _monitor_stale_instruments(self):
        """Monitor stale instruments"""
        if len(self.token_batches) == 0:
            self._build_tokens()
        tick_monitor = TickMonitor(token_batches=self.token_batches)
        while not self.stop_event.is_set():
            # Track processing rate
            await tick_monitor.monitor_stale_updates()
            await asyncio.sleep(60)

    def _flush_to_db(self, batch):
        """Bulk insert ticks to database"""
        try:
            self.db_conn.insert_ticks(batch)
        except Exception as e:
            self.logger.error(f"Database error: {str(e)}")

    def start(self):
        """Start WebSocket client and processing threads"""
        self.logger.debug("Starting Zerodha WebSocket client")

        # Create event loop for main thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Start WebSocket and other tasks
        self.ws_tasks = []
        for token_batch in self.token_batches:
            task = self.loop.create_task(self._connect_websocket([token_batch]))
            self.ws_tasks.append(task)

        self.monitor_task = self.loop.create_task(self._monitor_performance())
        self.monitor_stale_task = self.loop.create_task(
            self._monitor_stale_instruments()
        )
        self.conn_monitor_task = self.loop.create_task(self._connection_monitor())

        # Start processing thread (still needs to be thread)
        self.processor_thread = threading.Thread(
            target=self._process_ticks, daemon=True
        )
        self.processor_thread.start()

        # Run the event loop
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Graceful shutdown"""
        self.logger.debug("Stopping Zerodha WebSocket client")
        self.stop_event.set()

        # Close all database connections
        self.db_conn.close_all()

        # Cancel all tasks
        for task in self.ws_tasks:
            if task and not task.done():
                task.cancel()
        for task in [
            self.monitor_task,
            self.conn_monitor_task,
            self.monitor_stale_task,
        ]:
            if task and not task.done():
                task.cancel()

        # Stop the event loop
        if hasattr(self, "loop"):
            self.loop.stop()

        if hasattr(self, "watcher_queue"):
            if self.watcher_queue is not None:
                self.watcher_queue = None

        # Wait for processor thread
        if hasattr(self, "processor_thread"):
            self.processor_thread.join(timeout=5)

        self.logger.debug("Shutdown complete")
