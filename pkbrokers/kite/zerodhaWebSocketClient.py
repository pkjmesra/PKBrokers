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
from typing import List

import pytz
import websockets
from kiteconnect.ticker import KiteTicker
from PKDevTools.classes import Archiver, log
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities
from PKDevTools.classes.Environment import PKEnvironment

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

PING_INTERVAL = 15
PING_TIMEOUT = 5
 # With high tick frequency (e.g., 2000+ instruments, each tick 
 # every few hundred ms), 50k ticks can accumulate in seconds.
 # Large queues increase latency and memory pressure. They mask 
 # backpressure instead of solving the root cause. So, let's
 # keep it to 10k
OPTIMAL_MAX_QUEUE_SIZE = 32767 if sys.platform.startswith("darwin") else 100000
RECOVERY_COOLDOWN_SECONDS = 120
HTTP_400_429_WAIT_TIME = 120
STALE_THRESHOLD_SECONDS = 300
FULL_RESTART_STALE_THRESHOLD = 600
DROP_MONITOR_INTERVAL = 30          # seconds between checking drops
MAX_DROPS_PER_MINUTE = 50           # threshold for recovery
NETWORK_WAIT_TIME = 5
GENERAL_WAIT_TIME = 1
DB_BATCH_SIZE = 5000
BATCH_READ_SIZE = 500               # read up to 500 ticks per iteration
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

# At module level in zerodhaWebSocketClient.py
def _global_token_refresh_callback():
    """Global callback function that can be pickled"""
    try:
        from pkbrokers.kite.examples.externals import kite_auth
        from PKDevTools.classes.Environment import PKEnvironment
        kite_auth()
        return PKEnvironment().KTOKEN
    except Exception as e:
        return None

class WebSocketProcess:
    """
    Individual WebSocket connection process that handles its own token batch.
    """

    def __init__(self, enctoken, user_id, api_key, token_batch, websocket_index,
                 data_queue, stop_event, log_level=None, watcher_queue=None, 
                 token_refresh_callback=None, drop_counter=None, ws_stop_event=None):
        self.enctoken = enctoken
        self.token_refresh_callback = token_refresh_callback
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
        self.encToken_invalidated = False
        self.ws_stop_event = ws_stop_event
        self.drop_counter = drop_counter
        self._shutdown_requested = False
        self.multiprocessingForWindows()

    def _is_stop_requested(self):
        """Safely check if stop is requested without risking BrokenPipeError."""
        try:
            if self.stop_event and self.stop_event.is_set():
                return True
            if self.ws_stop_event:
                # Use a timeout to avoid hanging on broken pipe
                try:
                    return self.ws_stop_event.is_set()
                except (BrokenPipeError, EOFError, AttributeError, OSError):
                    # Parent process likely terminated
                    return True
        except (BrokenPipeError, EOFError, AttributeError, OSError):
            # Parent process likely terminated
            return True
        return False

    def _build_websocket_url(self):
        """Build WebSocket URL for this process."""
        if self.api_key is None or len(self.api_key) == 0:
            raise ValueError("API Key must not be blank")
        if self.user_id is None or len(self.user_id) == 0:
            raise ValueError("user_id must not be blank")
        # CRITICAL FIX: Try multiple sources for token
        enctoken = self.enctoken
        
        # If stored token is empty, try environment
        if not enctoken or len(enctoken) == 0:
            import os
            enctoken = os.environ.get("KTOKEN", "")
            
            # If still empty, try PKEnvironment
            if not enctoken or len(enctoken) == 0:
                try:
                    from PKDevTools.classes.Environment import PKEnvironment
                    enctoken = PKEnvironment().KTOKEN
                except:
                    pass
            
            # Update stored token if found
            if enctoken and len(enctoken) > 0:
                self.enctoken = enctoken
        
        if not enctoken or len(enctoken) == 0:
            # Log helpful debug info
            import os
            self.logger.error(f"🛑 🛑 🛑 🛑 KTOKEN in env: {bool(os.environ.get('KTOKEN'))}")
            try:
                from PKDevTools.classes.Environment import PKEnvironment
                self.logger.error(f"🛑 🛑 🛑 🛑 KTOKEN in PKEnvironment: {bool(PKEnvironment().KTOKEN)}")
            except:
                pass
            raise ValueError("enctoken must not be blank")

        base_params = {
            "api_key": self.api_key,
            "user_id": self.user_id,
            "enctoken": quote(enctoken),  # Use the resolved token
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
        if self._is_stop_requested():
            return

        if self.websocket_index == 0:
            # Subscribe to indices first
            self.logger.debug(
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

            self.logger.debug(
                f"Websocket_index:{self.websocket_index}: Batch size: {len(self.token_batch)}. Sending subscribe message: {subscribe_msg}"
            )
            await websocket.send(json.dumps(subscribe_msg))
            self.logger.debug(
                f"Websocket_index:{self.websocket_index}: Sending mode message: {mode_msg}"
            )
            await websocket.send(json.dumps(mode_msg))
            await asyncio.sleep(GENERAL_WAIT_TIME)

    async def _connect_websocket(self):
        """Establish and maintain WebSocket connection for this process."""
        while not self._is_stop_requested():
            try:
                async with websockets.connect(
                    self._build_websocket_url(),
                    extra_headers=self._build_headers(),
                    ping_interval=PING_INTERVAL,
                    ping_timeout=PING_TIMEOUT,
                    close_timeout=5,
                    compression="deflate",
                    max_size=2**17,
                ) as websocket:
                    self.logger.debug(
                        f"Websocket_index:{self.websocket_index}: Connected successfully"
                    )
                    self.websocket = websocket
                    
                    # Wait for initial messages
                    initial_messages = []
                    max_wait_counter = 2
                    wait_counter = 0
                    while len(initial_messages) < 2 and wait_counter < max_wait_counter and not self._is_stop_requested():
                        wait_counter += 1
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=10)
                        except asyncio.TimeoutError:
                            continue
                            
                        if isinstance(message, str):
                            data = json.loads(message)
                            if data.get("type") in ["instruments_meta", "app_code"]:
                                initial_messages.append(data)
                                self.logger.info(
                                    f"Websocket_index:{self.websocket_index}: Received initial message: {data}"
                                )
                            await asyncio.sleep(GENERAL_WAIT_TIME)

                    # Subscribe to instruments
                    await self._subscribe_instruments(websocket)

                    # Main message loop
                    last_heartbeat = time.time()
                    last_tick_log = time.time()
                    total_ticks_received = 0
                    tick_batch = 0
                    
                    while not self._is_stop_requested():
                        try:
                            message = await asyncio.wait_for(
                                websocket.recv(), timeout=10
                            )

                            if isinstance(message, bytes):
                                if len(message) == 1:
                                    continue  # Heartbeat, ignore

                                # Process market data
                                ticks = ZerodhaWebSocketParser.parse_binary_message(
                                    message,
                                    websocket_index=self.websocket_index,
                                    batch_index=self.websocket_index
                                )
                                total_ticks_received += len(ticks)
                                tick_batch += len(ticks)
                                
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
                                        "exchange_timestamp": tick.exchange_timestamp or PKDateUtilities.currentDateTimestamp(),
                                        "depth": tick.depth,
                                        "websocket_index": self.websocket_index,  # Already present
                                        "batch_index": self.websocket_index,      # Add batch index
                                    }
                                    
                                    # Safely put in queue (non-blocking)
                                    max_retries = 3
                                    for attempt in range(max_retries):
                                        try:
                                            self.data_queue.put(tick_data, timeout=1)
                                            break
                                        except Exception:
                                            if attempt == max_retries - 1:
                                                if not hasattr(self, '_last_queue_full_log') or time.time() - self._last_queue_full_log > 60:
                                                    self.logger.warning(f"⚠️ Queue full, dropping tick after {max_retries} attempts")
                                                    self._last_queue_full_log = time.time()
                                                # Optionally increment a shared counter
                                                if self.drop_counter is not None:
                                                    with self.drop_counter.get_lock():
                                                        self.drop_counter.value += 1
                                            else:
                                                await asyncio.sleep(0.05 * (2 ** attempt))

                            elif isinstance(message, str):
                                try:
                                    data = json.loads(message)
                                    # Handle text messages if needed
                                except json.JSONDecodeError:
                                    self.logger.warning(
                                        f"Websocket_index:{self.websocket_index}: Invalid JSON message: {message}"
                                    )

                            # Send heartbeat if needed
                            if time.time() - last_heartbeat > PING_INTERVAL:
                                await websocket.send(json.dumps({"a": "ping"}))
                                last_heartbeat = time.time()

                        except asyncio.TimeoutError:
                            await websocket.ping()
                            continue
                        except asyncio.exceptions.IncompleteReadError:
                            self.logger.warning(
                                f"Websocket_index:{self.websocket_index}: Connection lost (IncompleteReadError)"
                            )
                            break
                        except websockets.exceptions.ConnectionClosedError as e:
                            self.logger.warning(
                                f"Websocket_index:{self.websocket_index}: Connection closed: {e.code} - {e.reason}"
                            )
                            break
                        except Exception as e:
                            if not self._is_stop_requested():
                                self.logger.error(
                                    f"🛑 🛑 🛑 🛑 Websocket_index:{self.websocket_index}: Message processing error: {str(e)}"
                                )
                            break
                            
                    await self._async_cleanup()
                    
            except websockets.exceptions.ConnectionClosedError as e:
                if not self._is_stop_requested():
                    if hasattr(e, "code"):
                        self.logger.warning(
                            f"Websocket_index:{self.websocket_index}: Connection closed: {e.code} - {e.reason}"
                        )
                await asyncio.sleep(NETWORK_WAIT_TIME)
            except Exception as e:
                if not self._is_stop_requested():
                    self.logger.error(
                        f"🛑 🛑 🛑 🛑 Websocket_index:{self.websocket_index}: WebSocket connection error: {str(e)}. Reconnecting in {HTTP_400_429_WAIT_TIME} seconds..."
                    )
                if "http 400" in str(e).lower() or "http 429" in str(e).lower():
                    self.logger.warning(
                        f"Websocket_index:{self.websocket_index}: Hit rate limit or bad request. Waiting longer before reconnecting..."
                    )
                    await asyncio.sleep(HTTP_400_429_WAIT_TIME)
                if ("http 403" in str(e).lower() or "enctoken" in str(e).lower()) and not self.encToken_invalidated:
                    self.encToken_invalidated = True
                    # Try callback first (if available)
                    if self.token_refresh_callback:
                        try:
                            new_token = self.token_refresh_callback()
                            if new_token and len(new_token) > 0:
                                self.enctoken = new_token
                                self.logger.info("Token refreshed via callback")
                                self.encToken_invalidated = False  # Reset on success
                                continue  # Retry with new token
                        except Exception as callback_err:
                            self.logger.error(f"🛑 🛑 🛑 🛑 Token refresh callback failed: {callback_err}")
                    
                    # Fallback to direct auth if callback fails or not available
                    try:
                        from pkbrokers.kite.examples.externals import kite_auth
                        from PKDevTools.classes.Environment import PKEnvironment
                        kite_auth()
                        self.enctoken = PKEnvironment().KTOKEN
                        if self.enctoken and len(self.enctoken) > 0:
                            self.logger.info("Token refreshed via direct auth")
                            self.encToken_invalidated = False
                            continue
                    except Exception as auth_err:
                        self.logger.error(f"🛑 🛑 🛑 🛑 Direct auth failed: {auth_err}")
                await asyncio.sleep(NETWORK_WAIT_TIME)

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
        self._shutdown_requested = True
        try:
            asyncio.run(self._async_cleanup())
        except Exception:
            pass

    async def _async_cleanup(self):
        """Async cleanup tasks."""
        if hasattr(self, "websocket") and self.websocket:
            try:
                await self.websocket.close()
                self.logger.warning(f"⚠️ Websocket_index:{self.websocket_index} closed!")
            except BaseException:
                pass

    def run(self):
        """Main process entry point."""
        # Initialize process-specific logger
        self.setupLogger()
        self.logger = default_logger()
        self.logger.setLevel(self.log_level)
        self.logger.debug(
            f"Websocket_index:{self.websocket_index}: Starting WebSocket process."
        )
        try:
            asyncio.run(self._connect_websocket())
        except KeyboardInterrupt:
            self.logger.warning("⚠️ Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 WebSocket process error: {e}")
        finally:
            self.logger.warning(f"⚠️ Websocket_index:{self.websocket_index}: Process exiting")

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
        token_refresh_callback,
        drop_counter,
        ws_stop_event,
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
        token_refresh_callback=token_refresh_callback,
        drop_counter = drop_counter,
        ws_stop_event=ws_stop_event,
    )
    try:
        process.run()
    except Exception as e:
        from PKDevTools.classes.log import default_logger
        default_logger().error(f"🛑 🛑 🛑 🛑 WebSocket process {websocket_index} error: {e}")
    finally:
        if hasattr(process, "close"):
            try:
                process.close()
            except BaseException:
                pass


class ZerodhaWebSocketClient:
    """WebSocket client for Zerodha's trading API with multiprocessing support."""

    def __init__(self, enctoken, user_id, api_key="kitefront", token_batches=[], 
                 watcher_queue=None, db_conn=None, ws_stop_event=None):
        self.watcher_queue = watcher_queue
        self.enctoken = enctoken
        self.user_id = user_id
        self.api_key = api_key
        self.logger = default_logger()
        self.ws_stop_event = ws_stop_event
        self.ws_url = self._build_websocket_url()
        self.logger.debug(f"ZerodhaWebSocketClient initialized")
        self._restart_counter = {}

        self.mp_context = multiprocessing.get_context("spawn")
        # self.manager = self.mp_context.Manager()
        self.data_queue = self.mp_context.Queue(maxsize=OPTIMAL_MAX_QUEUE_SIZE)  # Limit queue size
        self.stop_event = self.mp_context.Event()

        self.db_conn = db_conn
        self.token_batches = token_batches
        self.ws_processes = []
        self._stop_requested = False
        self.token_refresh_callback = _global_token_refresh_callback

        # Track last tick time for each batch
        self.batch_last_tick_time = {}  # batch_index -> last tick timestamp
        self.batch_lock = threading.Lock()  # Thread-safe updates
        self.drop_counter = self.mp_context.Value('i', 0)   # shared integer, initial 0

    def _refresh_token(self):
        """Callback function to refresh token - runs in parent context"""
        try:
            from pkbrokers.kite.examples.externals import kite_auth
            from PKDevTools.classes.Environment import PKEnvironment
            
            # Refresh token
            kite_auth()
            new_token = PKEnvironment().KTOKEN
            
            if new_token and len(new_token) > 0:
                # Update all child processes' tokens
                self.enctoken = new_token
                
                # Also update environment for future processes
                import os
                os.environ["KTOKEN"] = new_token
                
                return new_token
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Token refresh failed: {e}")
        
        return None
    
    def _build_tokens(self):
        """Build token batches by fetching available instruments."""
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
        return Tick(
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
            websocket_index=tick_data.get("websocket_index", -1),
            batch_index=tick_data.get("batch_index", -1),
        )

    def _parse_binary_message(self, message: bytes) -> list:
        """Parse binary WebSocket message - wrapper around ZerodhaWebSocketParser."""
        return ZerodhaWebSocketParser.parse_binary_message(message)

    def _parse_single_packet(self, packet: bytes):
        """Parse single binary packet and return as Tick object."""
        return ZerodhaWebSocketParser._parse_single_packet(packet)

    def _parse_binary_packet(self, packet: bytes) -> dict:
        """Parse single binary packet and return as dictionary (for backward compatibility)."""
        tick = ZerodhaWebSocketParser._parse_single_packet(packet)
        if tick is None:
            return None
        # Convert Tick object to dictionary
        return {
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
            "exchange_timestamp": tick.exchange_timestamp,
            "depth": tick.depth,
            "websocket_index": tick.websocket_index,
            "batch_index": tick.batch_index,
        }

    def _build_websocket_url(self):
        """Build WebSocket URL - delegates to WebSocketProcess for testing."""
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
        """Generate WebSocket headers."""
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

    def _process_ticks(self):
        """Process ticks from queue in batches to reduce overhead."""
        batch = []
        self.last_consume_time = time.time()
        last_flush = time.time()
        tick_data = None

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # --- Read multiple ticks at once ---
                ticks_batch = []
                for _ in range(BATCH_READ_SIZE):
                    try:
                        tick_data = self.data_queue.get(timeout=0.01)
                    except queue.Empty:
                        if batch and (time.time() - last_flush > 5):
                            self._flush_to_db(batch)
                            batch = []
                            last_flush = time.time()
                            break
                    except ValueError as e:
                        if "multiprocessing.queues.Queue object" in str(e) and "is closed" in str(e):
                            self.stop_event.set()
                            self.stop()
                            break
                        continue
                    if tick_data and (isinstance(tick_data, dict) and tick_data.get("type") == "tick") or (isinstance(tick_data, Tick)):
                        ticks_batch.append(tick_data)
                    else:
                        # skip non‑tick data
                        pass

                if not ticks_batch:
                    # No ticks, small sleep to avoid busy loop
                    time.sleep(0.001)
                    continue

                # --- Process each tick in the batch ---
                for tick_data in ticks_batch:
                    # Update batch timestamp
                    batch_index = tick_data.get("websocket_index", -1)
                    if batch_index >= 0:
                        self.update_batch_tick_time(batch_index)

                    if tick_data["exchange_timestamp"] is None:
                        tick_data["exchange_timestamp"] = PKDateUtilities.currentDateTimestamp()

                    # Convert back to Tick object
                    tick = self._convert_tick_data_to_object(tick_data)
                    if tick.exchange_timestamp is None:
                        tick.exchange_timestamp = PKDateUtilities.currentDateTimestamp()

                    processed = {
                        "instrument_token": tick.instrument_token,
                        "timestamp": datetime.fromtimestamp(
                            tick.exchange_timestamp, tz=pytz.timezone("Asia/Kolkata")
                        ),
                        "last_price": tick.last_price or 0,
                        "day_volume": tick.day_volume or 0,
                        "oi": tick.oi or 0,
                        "buy_quantity": tick.buy_quantity or 0,
                        "sell_quantity": tick.sell_quantity or 0,
                        "high_price": tick.high_price or 0,
                        "low_price": tick.low_price or 0,
                        "open_price": tick.open_price or 0,
                        "prev_day_close": tick.prev_day_close or 0,
                        "websocket_index": tick.websocket_index,
                        "batch_index": tick.batch_index,
                    }

                    if tick.depth:
                        processed["depth"] = {
                            "bid": [
                                {
                                    "price": b.get("price", 0) if isinstance(b, dict) else (b.price or 0),
                                    "quantity": b.get("quantity", 0) if isinstance(b, dict) else (b.quantity or 0),
                                    "orders": b.get("orders", 0) if isinstance(b, dict) else (b.orders or 0),
                                }
                                for b in tick.depth.get("bid", [])[:5]
                            ],
                            "ask": [
                                {
                                    "price": a.get("price", 0) if isinstance(a, dict) else (a.price or 0),
                                    "quantity": a.get("quantity", 0) if isinstance(a, dict) else (a.quantity or 0),
                                    "orders": a.get("orders", 0) if isinstance(a, dict) else (a.orders or 0),
                                }
                                for a in tick.depth.get("ask", [])[:5]
                            ],
                        }

                    # Add to local batch for database flush
                    batch.append(processed)

                    # Send to watcher queue (KiteTokenWatcher)
                    if self.watcher_queue is not None:
                        self.watcher_queue.put(tick)

                    # Flush to database if batch size reached
                    if len(batch) >= DB_BATCH_SIZE:
                        self._flush_to_db(batch)
                        batch = []
                        last_flush = time.time()

                # Update last consume time after processing the batch
                self.last_consume_time = time.time()

                # Periodic flush for remaining ticks
                if batch and (time.time() - last_flush > 5):
                    self._flush_to_db(batch)
                    batch = []
                    last_flush = time.time()

            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Error processing ticks: {str(e)}")

        # Final flush
        if batch:
            self._flush_to_db(batch)

    def _flush_to_db(self, batch):
        """Bulk insert ticks to database."""
        return
        try:
            if self.db_conn:
                self.db_conn.insert_ticks(batch)
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Database error: {str(e)}")
            import traceback

            traceback.print_exc()

    def _restart_all_processes(self, process_args):
        self.logger.warning("🛑 Full restart – recreating shared drop counter")
        # Stop old processes
        for i, p in enumerate(self.ws_processes):
            if p and p.is_alive():
                p.terminate()
                p.join(timeout=5)
        # Create a new drop counter
        self.drop_counter = self.mp_context.Value('i', 0)
        # Refresh token
        self._refresh_token()
        # Rebuild process_args with new drop_counter
        new_process_args = []
        for base in process_args:
            base_list = list(base)
            base_list[9] = self.drop_counter
            new_process_args.append(tuple(base_list))
        # Restart all processes
        for i, base_args in enumerate(new_process_args):
            full_args = base_args + (self.ws_stop_event,)
            p = self.mp_context.Process(target=websocket_process_worker, args=(full_args,))
            p.daemon = True
            p.start()
            self.ws_processes[i] = p
            self.update_batch_tick_time(i)
        return new_process_args
            
    def _monitor_processes(self, process_args):
        """Monitor and restart failed processes."""
        try:
            last_drop_check = time.time()
            last_drop_count = 0
            while not self.stop_event.is_set():
                # --- Check external stop event safely ---
                try:
                    if self.ws_stop_event and self.ws_stop_event.is_set():
                        self.logger.warning("⚠️ External stop requested, shutting down WebSocket processes...")
                        self.stop_event.set()
                        break
                except (BrokenPipeError, EOFError, ConnectionError, AttributeError) as e:
                    self.logger.warning(f"⚠️ Ignoring broken pipe while checking ws_stop_event: {e}")
                    # Do NOT break – continue monitoring
                except Exception as e:
                    self.logger.error(f"🛑 🛑 🛑 🛑 Unexpected error checking ws_stop_event: {e}", exc_info=True)
                # --- Restart dead processes ---
                for i, p in enumerate(self.ws_processes):
                    if p and not p.is_alive():
                        # Restart counter logic
                        now = time.time()
                        last = getattr(self, '_restart_counter', {}).get(i, 0)
                        if now - last < 60:
                            count = getattr(self, '_restart_counter', {}).get(f'{i}_count', 0) + 1
                            if count > 3:
                                self.logger.error(f"Process {i} restarting too often – triggering full restart")
                                self._restart_all_processes(process_args)
                                # Reset local monitoring variables
                                last_drop_check = time.time()
                                last_drop_count = self.drop_counter.value
                                return   # exit monitor; restart_all will restart everything
                            self._restart_counter[f'{i}_count'] = count
                        else:
                            self._restart_counter[f'{i}_count'] = 1
                        self._restart_counter[i] = now

                        # Now restart the process
                        exitcode = p.exitcode
                        self.logger.warning(f"⚠️ Websocket_index:{i} died (exitcode: {exitcode}), restarting...")
                        base_args = process_args[i]
                        full_args = base_args + (self.ws_stop_event,)
                        new_p = self.mp_context.Process(target=websocket_process_worker, args=(full_args,))
                        new_p.daemon = True
                        new_p.start()
                        self.ws_processes[i] = new_p
                        self.update_batch_tick_time(i)

                current_time = time.time()
                if current_time < self._initial_grace_period:
                    # Skip stale checks during initial grace period
                    time.sleep(NETWORK_WAIT_TIME)
                    continue
                # --- Check stale batches and restart ---
                for i, batch_tokens in enumerate(self.token_batches):
                    last_tick = self.batch_last_tick_time.get(i, 0)
                    stale_duration = current_time - last_tick
                    if stale_duration > STALE_THRESHOLD_SECONDS:
                        self.logger.warning(
                            f"⚠️ Batch {i} is stale (no ticks for {stale_duration:.0f}s, "
                            f"threshold {STALE_THRESHOLD_SECONDS}s) → restarting process"
                        )
                        # Track how long this batch has been continuously stale
                        if not hasattr(self, '_batch_stale_start'):
                            self._batch_stale_start = {}
                        if i not in self._batch_stale_start:
                            self._batch_stale_start[i] = current_time
                        else:
                            total_stale = current_time - self._batch_stale_start[i]
                            if total_stale > FULL_RESTART_STALE_THRESHOLD:
                                self.logger.error(
                                    f"🛑 Batch {i} has been stale for {total_stale:.0f}s "
                                    f"(>{FULL_RESTART_STALE_THRESHOLD}s) → triggering full restart"
                                )
                                self._restart_all_processes(process_args)
                                # Reset local drop monitoring variables to avoid false trigger
                                last_drop_check = time.time()
                                last_drop_count = self.drop_counter.value
                                continue  # skip remaining checks this iteration

                        # Restart individual process
                        if i < len(self.ws_processes) and self.ws_processes[i] and self.ws_processes[i].is_alive():
                            self.logger.info(f"Terminating stale batch process {i} (PID {self.ws_processes[i].pid})")
                            self.ws_processes[i].terminate()
                            self.ws_processes[i].join(timeout=5)
                        # Restart the process
                        base_args = process_args[i]
                        full_args = base_args + (self.ws_stop_event,)
                        new_p = self.mp_context.Process(target=websocket_process_worker, args=(full_args,))
                        new_p.daemon = True
                        new_p.start()
                        self.ws_processes[i] = new_p
                        self.update_batch_tick_time(i)   # reset after restart
                        self.logger.info(f"✅ Restarted batch {i} process (new PID {new_p.pid})")
                    else:
                        # Batch is healthy, clear stale start marker if any
                        if hasattr(self, '_batch_stale_start') and i in self._batch_stale_start:
                            del self._batch_stale_start[i]
                # --- Full restart if all batches dead (optional) ---
                # Every RECOVERY_COOLDOWN_SECONDS seconds, check total ticks across all processes
                if int(time.time()) % RECOVERY_COOLDOWN_SECONDS == 0:
                    active_batches = sum(1 for t in self.batch_last_tick_time.values()
                                        if time.time() - t < STALE_THRESHOLD_SECONDS)
                    if active_batches == 0 and not self._stop_requested:
                        self.logger.error("🛑 All batches dead – forcing full restart")
                        self._restart_all_processes(process_args)
                        # reset local timers and continue
                        last_drop_check = time.time()
                        last_drop_count = self.drop_counter.value
                        continue
                
                # Check that the processor thread is still alive
                if hasattr(self, 'processor_thread') and not self.processor_thread.is_alive():
                    self.logger.error("🛑 🛑 🛑 🛑 Processor thread died! Restarting client...")
                    self._restart_all_processes(process_args)
                    # reset local timers and continue
                    last_drop_check = time.time()
                    last_drop_count = self.drop_counter.value
                    break

                # Monitor drop counter
                if time.time() - last_drop_check >= DROP_MONITOR_INTERVAL:
                    current_drops = self.drop_counter.value
                    drops_since_last = current_drops - last_drop_count
                    minutes = DROP_MONITOR_INTERVAL / 60.0
                    drops_per_minute = drops_since_last / minutes if minutes > 0 else 0

                    if drops_per_minute > MAX_DROPS_PER_MINUTE:
                        self.logger.error(
                            f"🚨 Excessive tick drops detected: {drops_since_last} drops in {DROP_MONITOR_INTERVAL}s "
                            f"({drops_per_minute:.1f}/min). Triggering full restart."
                        )
                        self._restart_all_processes(process_args)
                        # Reset local monitoring variables
                        last_drop_check = time.time()
                        last_drop_count = self.drop_counter.value
                        continue

                    # Reset for next interval
                    last_drop_count = current_drops
                    last_drop_check = time.time()

                # If the thread is alive but stuck (e.g., deadlock on a lock), no recovery occurs.
                # Add a watchdog that checks the last time a tick was successfully retrieved from data_queue
                if self.last_consume_time and (time.time() - self.last_consume_time) > STALE_THRESHOLD_SECONDS:
                    self.logger.error("🛑 No ticks received for a long time – triggering restart")
                    self._restart_all_processes(process_args)
                    last_drop_check = time.time()
                    last_drop_count = self.drop_counter.value
                    continue
                
                time.sleep(NETWORK_WAIT_TIME)

        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Monitor loop fatal error ❌❌❌❌: {e}", exc_info=True)
        finally:
            self.stop()

    def get_batch_last_tick_times(self):
        """Return copy of batch_last_tick_time dictionary."""
        with self.batch_lock:
            return self.batch_last_tick_time.copy()

    def get_unhealthy_batches(self, current_time: float, stale_threshold: int) -> List[int]:
        """Return list of batch indices that haven't received ticks recently."""
        unhealthy = []
        with self.batch_lock:
            for batch_idx, last_time in self.batch_last_tick_time.items():
                if current_time - last_time > stale_threshold:
                    unhealthy.append(batch_idx)
        return unhealthy

    def update_batch_tick_time(self, batch_index: int):
        """Update the last tick time for a specific batch."""
        with self.batch_lock:
            self.batch_last_tick_time[batch_index] = time.time()
    
    def start(self):
        """Start WebSocket client with multiprocessing."""
        self.logger.debug("Starting Zerodha WebSocket client")
        self._initial_grace_period = time.time() + STALE_THRESHOLD_SECONDS
        # Validate token before starting any processes
        from PKDevTools.classes.Environment import PKEnvironment
        token = PKEnvironment().KTOKEN
        
        if not token or len(token) < 10:
            self.logger.warning("⚠️ Token invalid, refreshing...")
            try:
                from pkbrokers.kite.examples.externals import kite_auth
                kite_auth()
                token = PKEnvironment().KTOKEN
                if not token or len(token) < 10:
                    self.logger.error("🛑 🛑 🛑 🛑 No valid KTOKENeven after kite_auth – cannot start WebSocket client")
                    raise ValueError("STILL Missing KTOKEN")
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Failed to refresh token: {e}")
                return
        
        # Pass token to all child processes
        self.enctoken = token  # Store in parent
        if len(self.token_batches) == 0:
            self._build_tokens()

        num_batches = len(self.token_batches)
        total_instruments = sum(len(batch) for batch in self.token_batches)
        self.logger.debug(f"Starting {num_batches} processes for {total_instruments} instruments")

        # Start processing thread
        self.processor_thread = threading.Thread(target=self._process_ticks, daemon=True)
        self.processor_thread.start()

        # Prepare arguments for each batch - one process per batch
        # Each batch is already limited to OPTIMAL_TOKEN_BATCH_SIZE (500) instruments
        # We need one WebSocket connection per batch (Zerodha limit)
        process_args = []
        for i in range(num_batches):
            token_batch = self.token_batches[i]
            self.logger.debug(f"Batch {i}: {len(token_batch)} instruments")
            
            base_args = (
                self.enctoken,
                self.user_id,
                self.api_key,
                token_batch,
                i,
                self.data_queue,
                self.stop_event,
                0 if "PKDevTools_Default_Log_Level" not in os.environ.keys()
                else int(os.environ["PKDevTools_Default_Log_Level"]),
                self.token_refresh_callback,
                self.drop_counter,
            )
            process_args.append(base_args)

        self.ws_processes = []
        for base_args in process_args:
            full_args = base_args + (self.ws_stop_event,)
            p = self.mp_context.Process(
                target=websocket_process_worker, 
                args=(full_args,)
            )
            p.daemon = True
            p.start()
            self.ws_processes.append(p)

        # Initialize batch_last_tick_time for all batches to current time
        for i in range(num_batches):
            self.update_batch_tick_time(i)
        self._monitor_processes(process_args)

    def stop(self):
        """Graceful shutdown of the WebSocket client."""
        if self._stop_requested:
            return
        self._stop_requested = True
        
        self.logger.warning("⚠️ Stopping WebSocket client")
        try:
            self.stop_event.set()
        except:
            pass

        # Close the queue to unblock any waiting gets
        try:
            self.data_queue.close()
            self.data_queue.join_thread()
        except Exception:
            pass
        for i, p in enumerate(self.ws_processes):
            if p and p.is_alive():
                p.join(timeout=10)
                if p.is_alive():
                    self.logger.warning(f"⚠️ Process {i} not responding, terminating...")
                    p.terminate()
                    p.join(timeout=5)

        if self.db_conn:
            self.db_conn.close_all()

        if hasattr(self, "processor_thread") and self.processor_thread.is_alive():
            self.processor_thread.join(timeout=5)

        if hasattr(self, "watcher_queue") and self.watcher_queue is not None:
            self.watcher_queue = None

        # # Close the manager
        # if hasattr(self, "manager"):
        #     self.manager.shutdown()

        self.logger.warning("⚠️ Shutdown complete")
