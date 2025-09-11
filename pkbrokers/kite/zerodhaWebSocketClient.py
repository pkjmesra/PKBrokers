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
import multiprocessing
import os
import queue
import sys
import threading
import time
from datetime import datetime
from urllib.parse import quote

import pytz
import websockets
from kiteconnect.ticker import KiteTicker
from PKDevTools.classes import Archiver, log
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities

from pkbrokers.kite.ticks import Tick
from pkbrokers.kite.zerodhaWebSocketParser import ZerodhaWebSocketParser

if __name__ == "__main__":
    multiprocessing.freeze_support()

try:
    # Python 3.4+
    if sys.platform.startswith("win"):
        import multiprocessing.popen_spawn_win32 as forking
    else:
        import multiprocessing.popen_fork as forking
except ImportError:
    print("Contact developer! Your platform does not support multiprocessing!")


DEFAULT_PATH = Archiver.get_user_data_dir()

PING_INTERVAL = 30
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


class WebSocketProcess:
    """
    Individual WebSocket connection process that handles its own token batch.
    """

    def __init__(
        self,
        enctoken,
        user_id,
        api_key,
        token_batch,
        websocket_index,
        data_queue,
        stop_event,
        log_level=None,
        watcher_queue=None,
    ):
        self.enctoken = enctoken
        self.user_id = user_id
        self.api_key = api_key
        self.token_batch = token_batch
        self.websocket_index = websocket_index
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.watcher_queue = watcher_queue
        self.log_level = log_level
        self.logger = None
        self.websocket = None
        self.multiprocessingForWindows()

    def _build_websocket_url(self):
        """Build WebSocket URL for this process."""
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
        """Generate WebSocket headers for this process."""
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

    async def _subscribe_instruments(self, websocket, subscribe_all_indices=False):
        """Subscribe to instruments for this process."""
        if self.stop_event.is_set():
            return

        if self.websocket_index == 0:
            # Subscribe to indices first
            self.logger.info(
                f"Websocket_index:{self.websocket_index}: Subscribing for indices"
            )

            # Subscribe to Nifty 50 index
            self.logger.debug(
                f"Websocket_index:{self.websocket_index}: Sending NIFTY_50 subscribe and mode messages"
            )
            await websocket.send(json.dumps({"a": "subscribe", "v": NIFTY_50}))
            await websocket.send(
                json.dumps({"a": "mode", "v": [KiteTicker.MODE_FULL, NIFTY_50]})
            )

            # Subscribe to BSE Sensex
            self.logger.debug(
                f"Websocket_index:{self.websocket_index}: Sending BSE_SENSEX subscribe and mode messages"
            )
            await websocket.send(json.dumps({"a": "subscribe", "v": BSE_SENSEX}))
            await websocket.send(json.dumps({"a": "mode", "v": ["full", BSE_SENSEX]}))

            if subscribe_all_indices:
                self.logger.debug(
                    f"Websocket_index:{self.websocket_index}: Sending OTHER_INDICES subscribe and mode messages"
                )
                await websocket.send(json.dumps({"a": "subscribe", "v": OTHER_INDICES}))
                await websocket.send(
                    json.dumps({"a": "mode", "v": ["full", OTHER_INDICES]})
                )

        # Subscribe to the token batch for this process
        if self.token_batch:
            subscribe_msg = {"a": "subscribe", "v": self.token_batch}
            mode_msg = {"a": "mode", "v": ["full", self.token_batch]}

            self.logger.info(
                f"Websocket_index:{self.websocket_index}: Batch size: {len(self.token_batch)}. Sending subscribe message: {subscribe_msg}"
            )
            await websocket.send(json.dumps(subscribe_msg))
            self.logger.debug(
                f"Websocket_index:{self.websocket_index}: Sending mode message: {mode_msg}"
            )
            await websocket.send(json.dumps(mode_msg))
            await asyncio.sleep(1)  # Respect rate limits

    async def _connect_websocket(self):
        """Establish and maintain WebSocket connection for this process."""
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    self._build_websocket_url(),
                    extra_headers=self._build_headers(),
                    ping_interval=PING_INTERVAL,
                    ping_timeout=10,
                    close_timeout=5,
                    compression="deflate",
                    max_size=2**17,
                ) as websocket:
                    self.logger.info(
                        f"Websocket_index:{self.websocket_index}: Connected successfully"
                    )
                    self.websocket = websocket
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
                                self.logger.info(
                                    f"Websocket_index:{self.websocket_index}: Received initial message: {data}"
                                )
                            await asyncio.sleep(1)

                    # Subscribe to instruments
                    await self._subscribe_instruments(websocket)

                    # Main message loop
                    last_heartbeat = time.time()
                    last_tick_log = time.time()
                    total_ticks_received = 0
                    tick_batch = 0
                    while not self.stop_event.is_set():
                        try:
                            message = await asyncio.wait_for(
                                websocket.recv(), timeout=10
                            )

                            if isinstance(message, bytes):
                                if len(message) == 1:
                                    continue  # Heartbeat, ignore

                                # Process market data
                                ticks = ZerodhaWebSocketParser.parse_binary_message(
                                    message
                                )
                                total_ticks_received += len(ticks)
                                tick_batch += len(ticks)
                                # if tick_batch > 0 and tick_batch % 200 >= 0:
                                if time.time() - last_tick_log > 4 * PING_INTERVAL:
                                    self.logger.info(
                                        f"Websocket_index:{self.websocket_index}: Total Running Count of Ticks:{total_ticks_received}"
                                    )
                                    tick_batch = 0
                                    last_tick_log = time.time()

                                for tick in ticks:
                                    # Put tick data as a dictionary to avoid pickling issues
                                    tick_data = {
                                        "type": "tick",
                                        "instrument_token": tick.instrument_token,
                                        "last_price": tick.last_price,
                                        "last_quantity": tick.last_quantity,
                                        "avg_price": tick.avg_price,
                                        "day_volume": tick.day_volume,
                                        "buy_quantity": tick.buy_quantity,
                                        "sell_quantity": tick.sell_quantity,
                                        "open_price": tick.open_price,
                                        "high_price": tick.high_price,
                                        "low_price": tick.low_price,
                                        "prev_day_close": tick.prev_day_close,
                                        "last_trade_timestamp": tick.last_trade_timestamp,
                                        "oi": tick.oi,
                                        "oi_day_high": tick.oi_day_high,
                                        "oi_day_low": tick.oi_day_low,
                                        "exchange_timestamp": tick.exchange_timestamp
                                        or PKDateUtilities.currentDateTimestamp(),
                                        "depth": tick.depth,
                                        "websocket_index": self.websocket_index,
                                    }
                                    self.data_queue.put(tick_data)

                            elif isinstance(message, str):
                                try:
                                    data = json.loads(message)
                                    # Handle text messages if needed
                                except json.JSONDecodeError:
                                    self.logger.warn(
                                        f"Websocket_index:{self.websocket_index}: Invalid JSON message: {message}"
                                    )

                            # Send heartbeat if needed
                            if time.time() - last_heartbeat > PING_INTERVAL:
                                await websocket.send(json.dumps({"a": "ping"}))
                                last_heartbeat = time.time()

                        except asyncio.TimeoutError:
                            await websocket.ping()
                        except Exception as e:
                            if not self.stop_event.is_set():
                                self.logger.error(
                                    f"Websocket_index:{self.websocket_index}: Message processing error: {str(e)}"
                                )
                            break
                await self._async_cleanup()
            # except asyncio.CancelledError:
            #     raise  # Propagate cancellation
            except websockets.exceptions.ConnectionClosedError as e:
                if hasattr(e, "code"):
                    self.logger.error(
                        f"Websocket_index:{self.websocket_index}: Connection closed: {e.code} - {e.reason}"
                    )
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(
                    f"Websocket_index:{self.websocket_index}: WebSocket connection error: {str(e)}. Reconnecting in 5 seconds..."
                )
                await asyncio.sleep(5)

    def setupLogger(self):
        if self.log_level > 0:
            os.environ["PKDevTools_Default_Log_Level"] = str(self.log_level)
        log.setup_custom_logger(
            "pkbrokers",
            self.log_level,
            trace=False,
            log_file_path="PKBrokers-log.txt",
            filter=None,
        )

    def close(self):
        """Synchronous cleanup method."""
        asyncio.run(self._async_cleanup())

    async def _async_cleanup(self):
        """Async cleanup tasks."""
        if hasattr(self, "websocket") and self.websocket:
            try:
                await self.websocket.close()
                self.logger.warn(f"Websocket_index:{self.websocket_index} closed!")
            except BaseException:
                pass

    def run(self):
        """Main process entry point."""
        # Initialize process-specific logger
        self.setupLogger()
        self.logger = default_logger()
        self.logger.setLevel(self.log_level)
        self.logger.info(
            f"Websocket_index:{self.websocket_index}: Starting WebSocket process."
        )
        asyncio.run(self._connect_websocket())

    def multiprocessingForWindows(self):
        if sys.platform.startswith("win"):
            # First define a modified version of Popen.
            class _Popen(forking.Popen):
                def __init__(self, *args, **kw):
                    if hasattr(sys, "frozen"):
                        # We have to set original _MEIPASS2 value from sys._MEIPASS
                        # to get --onefile mode working.
                        os.putenv("_MEIPASS2", sys._MEIPASS)
                    try:
                        super(_Popen, self).__init__(*args, **kw)
                    finally:
                        if hasattr(sys, "frozen"):
                            # On some platforms (e.g. AIX) 'os.unsetenv()' is not
                            # available. In those cases we cannot delete the variable
                            # but only set it to the empty string. The bootloader
                            # can handle this case.
                            if hasattr(os, "unsetenv"):
                                os.unsetenv("_MEIPASS2")
                            else:
                                os.putenv("_MEIPASS2", "")

            # Second override 'Popen' class with our modified version.
            forking.Popen = _Popen


def websocket_process_worker(args):
    """Worker function for multiprocessing that creates and runs WebSocketProcess."""
    (
        enctoken,
        user_id,
        api_key,
        token_batch,
        websocket_index,
        data_queue,
        stop_event,
        log_level,
    ) = args

    process = WebSocketProcess(
        enctoken=enctoken,
        user_id=user_id,
        api_key=api_key,
        token_batch=token_batch,
        websocket_index=websocket_index,
        data_queue=data_queue,
        stop_event=stop_event,
        log_level=log_level,
    )
    try:
        process.run()
    except Exception as e:
        print(f"WebSocket process {websocket_index} error: {e}")
    finally:
        # Ensure clean shutdown
        if hasattr(process, "close"):
            try:
                process.close()
            except BaseException:
                pass
        print(f"WebSocket process {websocket_index} stopped")


class ZerodhaWebSocketClient:
    """
    A WebSocket client for connecting to Zerodha's trading API to receive real-time market data.
    Now uses multiprocessing to handle each WebSocket connection in separate processes.
    """

    def __init__(
        self,
        enctoken,
        user_id,
        api_key="kitefront",
        token_batches=[],
        watcher_queue=None,
        db_conn=None,
    ):
        self.watcher_queue = watcher_queue
        self.enctoken = enctoken
        self.user_id = user_id
        self.api_key = api_key
        self.logger = default_logger()

        # Use consistent multiprocessing context
        self.mp_context = multiprocessing.get_context(
            "spawn"
            if sys.platform.startswith("darwin")
            else "spawn"  # if not sys.platform.startswith("darwin") else "spawn"
        )
        self.manager = self.mp_context.Manager()
        self.data_queue = self.manager.Queue(maxsize=0)
        self.stop_event = self.mp_context.Event()

        self.db_conn = db_conn
        self.token_batches = token_batches
        self.token_timestamp = 0
        self.ws_processes = []
        self.process_pool = None

    def _build_tokens(self):
        """Build token batches by fetching available instruments from Zerodha."""
        import os

        from PKDevTools.classes.Environment import PKEnvironment

        from pkbrokers.kite.instruments import KiteInstruments

        API_KEY = "kitefront"
        ACCESS_TOKEN = ""
        try:
            local_secrets = PKEnvironment().allSecrets
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

    def _convert_tick_data_to_object(self, tick_data):
        """Convert tick data dictionary back to Tick object."""
        # Create a Tick object with all required parameters
        tick = Tick(
            instrument_token=tick_data.get("instrument_token", 0),
            last_price=tick_data.get("last_price", 0),
            last_quantity=tick_data.get("last_quantity", 0),
            avg_price=tick_data.get("avg_price", 0),
            day_volume=tick_data.get("day_volume", 0),
            buy_quantity=tick_data.get("buy_quantity", 0),
            sell_quantity=tick_data.get("sell_quantity", 0),
            open_price=tick_data.get("open_price", 0),
            high_price=tick_data.get("high_price", 0),
            low_price=tick_data.get("low_price", 0),
            prev_day_close=tick_data.get("prev_day_close", 0),
            last_trade_timestamp=tick_data.get(
                "last_trade_timestamp", PKDateUtilities.currentDateTimestamp()
            ),
            oi=tick_data.get("oi", 0),
            oi_day_high=tick_data.get("oi_day_high", 0),
            oi_day_low=tick_data.get("oi_day_low", 0),
            exchange_timestamp=tick_data.get(
                "exchange_timestamp", PKDateUtilities.currentDateTimestamp()
            ),
            depth=tick_data.get("depth", {}),
        )
        return tick

    def _process_ticks(self):
        """Process ticks from queue and store in database."""
        batch = []

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # Use non-blocking get with timeout
                try:
                    tick_data = self.data_queue.get(timeout=1)
                except queue.Empty:
                    # Flush any remaining ticks if queue is empty
                    if batch:
                        self._flush_to_db(batch)
                        batch = []
                    continue

                if tick_data is None or tick_data.get("type") != "tick":
                    continue
                if tick_data["exchange_timestamp"] is None:
                    tick_data["exchange_timestamp"] = (
                        PKDateUtilities.currentDateTimestamp()
                    )
                # Convert back to Tick object for processing
                tick = self._convert_tick_data_to_object(tick_data)

                if tick.exchange_timestamp is None:
                    tick.exchange_timestamp = PKDateUtilities.currentDateTimestamp()

                processed = {
                    "instrument_token": tick.instrument_token,
                    "timestamp": datetime.fromtimestamp(
                        tick.exchange_timestamp, tz=pytz.timezone("Asia/Kolkata")
                    ),
                    "last_price": tick.last_price if tick.last_price is not None else 0,
                    "day_volume": tick.day_volume if tick.day_volume is not None else 0,
                    "oi": tick.oi if tick.oi is not None else 0,
                    "buy_quantity": tick.buy_quantity
                    if tick.buy_quantity is not None
                    else 0,
                    "sell_quantity": tick.sell_quantity
                    if tick.sell_quantity is not None
                    else 0,
                    "high_price": tick.high_price if tick.high_price is not None else 0,
                    "low_price": tick.low_price if tick.low_price is not None else 0,
                    "open_price": tick.open_price if tick.open_price is not None else 0,
                    "prev_day_close": tick.prev_day_close
                    if tick.prev_day_close is not None
                    else 0,
                }

                if tick.depth:
                    processed["depth"] = {
                        "bid": [
                            {
                                "price": b.get("price", 0)
                                if isinstance(b, dict)
                                else (b.price if b.price is not None else 0),
                                "quantity": b.get("quantity", 0)
                                if isinstance(b, dict)
                                else (b.quantity if b.quantity is not None else 0),
                                "orders": b.get("orders", 0)
                                if isinstance(b, dict)
                                else (b.orders if b.orders is not None else 0),
                            }
                            for b in tick.depth.get("bid", [])[:5]
                        ],
                        "ask": [
                            {
                                "price": a.get("price", 0)
                                if isinstance(a, dict)
                                else (a.price if a.price is not None else 0),
                                "quantity": a.get("quantity", 0)
                                if isinstance(a, dict)
                                else (a.quantity if a.quantity is not None else 0),
                                "orders": a.get("orders", 0)
                                if isinstance(a, dict)
                                else (a.orders if a.orders is not None else 0),
                            }
                            for a in tick.depth.get("ask", [])[:5]
                        ],
                    }
                batch.append(processed)

                # Send to watcher queue if available
                if self.watcher_queue is not None:
                    self.watcher_queue.put(tick)

                self._flush_to_db(batch)
                batch = []

            except Exception as e:
                self.logger.error(f"Error processing ticks: {str(e)}")

        # Flush any remaining ticks
        if batch:
            self._flush_to_db(batch)

    def _flush_to_db(self, batch):
        """Bulk insert ticks to database."""
        try:
            if self.db_conn:
                self.db_conn.insert_ticks(batch)
        except Exception as e:
            self.logger.error(f"Database error: {str(e)}")
            import traceback

            traceback.print_exc()

    def _monitor_processes(self, process_args):
        """Monitor and restart failed processes"""
        try:
            while not self.stop_event.is_set():
                # Check WebSocket processes
                for i, p in enumerate(self.ws_processes):
                    if not p.is_alive():
                        self.logger.warn(f"Websocket_index:{i} died, restarting...")
                        args = process_args[i]
                        new_p = self.mp_context.Process(
                            target=websocket_process_worker, args=(args,)
                        )
                        new_p.daemon = True
                        new_p.start()
                        self.ws_processes[i] = new_p

                time.sleep(5)

        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            self.logger.error(f"Error in multiprocessing: {str(e)}")
            self.stop()

    def start(self):
        """Start WebSocket client with multiprocessing."""
        self.logger.debug("Starting Zerodha WebSocket client with multiprocessing")

        # Build tokens if not provided
        if len(self.token_batches) == 0:
            self._build_tokens()

        # Determine optimal number of processes
        max_processes = multiprocessing.cpu_count()
        num_batches = len(self.token_batches)
        num_processes = min(max_processes, num_batches)

        self.logger.info(
            f"Starting {num_processes} WebSocket processes for {num_batches} token batches"
        )

        # Start processing thread for database inserts first
        self.processor_thread = threading.Thread(
            target=self._process_ticks, daemon=True
        )
        self.processor_thread.start()

        # Prepare arguments for each process
        process_args = []
        for i in range(num_processes):
            token_batch = self.token_batches[i] if i < num_batches else []
            args = (
                self.enctoken,
                self.user_id,
                self.api_key,
                token_batch,
                i,
                self.data_queue,
                self.stop_event,
                0
                if "PKDevTools_Default_Log_Level" not in os.environ.keys()
                else int(os.environ["PKDevTools_Default_Log_Level"]),
            )
            process_args.append(args)

        # Start WebSocket processes using the same context
        self.ws_processes = []

        for args in process_args:
            p = self.mp_context.Process(target=websocket_process_worker, args=(args,))
            p.daemon = True
            p.start()
            self.ws_processes.append(p)

        # Monitor processes
        self._monitor_processes(process_args)

    def stop(self):
        """Graceful shutdown of the WebSocket client."""
        self.logger.warn("Stopping Zerodha WebSocket client")
        self.stop_event.set()

        # Terminate all processes
        for i, p in enumerate(self.ws_processes):
            if p.is_alive():
                # Wait for graceful shutdown
                p.join(timeout=10)  # Increased timeout for graceful shutdown

            # Force terminate if still alive after graceful period
            if p.is_alive():
                self.logger.warn(f"Process {i} not responding, terminating...")
                p.terminate()
                p.join(timeout=5)

        # Close database connections
        if self.db_conn:
            self.db_conn.close_all()

        # Wait for processor thread
        if hasattr(self, "processor_thread"):
            self.processor_thread.join(timeout=5)

        if hasattr(self, "watcher_queue") and self.watcher_queue is not None:
            self.watcher_queue = None

        # Close the manager
        if hasattr(self, "manager"):
            self.manager.shutdown()

        self.logger.warn("Shutdown complete")
