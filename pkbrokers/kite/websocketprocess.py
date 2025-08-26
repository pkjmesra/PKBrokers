import asyncio
import base64
import json
import os
import time
from urllib.parse import quote

import websockets
from PKDevTools.classes import log
from PKDevTools.classes.log import default_logger

from pkbrokers.kite.zerodhaWebSocketParser import ZerodhaWebSocketParser

PING_INTERVAL = 30
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

        # Subscribe to indices first
        self.logger.info(
            f"Subscribing for indices on websocket_index:{self.websocket_index}"
        )

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

        # Subscribe to the token batch for this process
        if self.token_batch:
            self.logger.info(
                f"Subscribing for batch on websocket_index:{self.websocket_index}"
            )
            subscribe_msg = {"a": "subscribe", "v": self.token_batch}
            mode_msg = {"a": "mode", "v": ["full", self.token_batch]}

            self.logger.info(
                f"Batch size: {len(self.token_batch)}. Sending subscribe message: {subscribe_msg}"
            )
            await websocket.send(json.dumps(subscribe_msg))
            self.logger.debug(f"Sending mode message: {mode_msg}")
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
                    self.logger.debug(
                        f"WebSocket {self.websocket_index} connected successfully"
                    )

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
                                self.logger.info(
                                    f"Received on websocket_index:{self.websocket_index}, initial message: {data}"
                                )
                            await asyncio.sleep(1)

                    # Subscribe to instruments
                    await self._subscribe_instruments(websocket)

                    # Main message loop
                    last_heartbeat = time.time()

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
                                self.logger.info(
                                    f"Received on websocket_index:{self.websocket_index}, Ticks:{len(ticks)}"
                                )
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
                                        "exchange_timestamp": tick.exchange_timestamp,
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
                                        f"Invalid JSON message on websocket_index:{self.websocket_index}: {message}"
                                    )

                            # Send heartbeat if needed
                            if time.time() - last_heartbeat > PING_INTERVAL:
                                await websocket.send(json.dumps({"a": "ping"}))
                                last_heartbeat = time.time()

                        except asyncio.TimeoutError:
                            await websocket.ping()
                        except Exception as e:
                            self.logger.error(
                                f"Message processing error on websocket_index:{self.websocket_index}: {str(e)}"
                            )
                            break

            except websockets.exceptions.ConnectionClosedError as e:
                if hasattr(e, "code"):
                    self.logger.error(f"Connection closed: {e.code} - {e.reason}")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(
                    f"WebSocket connection error: {str(e)}. Reconnecting in 5 seconds..."
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

    def run(self):
        """Main process entry point."""
        # Initialize process-specific logger
        self.setupLogger()
        self.logger = default_logger()

        self.logger.info(f"Starting WebSocket process {self.websocket_index}")
        asyncio.run(self._connect_websocket())


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
    process.run()
