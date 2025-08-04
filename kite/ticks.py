# -*- coding: utf-8 -*-
# """
#     The MIT License (MIT)

#     Copyright (c) 2023 pkjmesra

#     Permission is hereby granted, free of charge, to any person obtaining a copy
#     of this software and associated documentation files (the "Software"), to deal
#     in the Software without restriction, including without limitation the rights
#     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#     copies of the Software, and to permit persons to whom the Software is
#     furnished to do so, subject to the following conditions:

#     The above copyright notice and this permission notice shall be included in all
#     copies or substantial portions of the Software.

#     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#     IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#     FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#     LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#     OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#     SOFTWARE.

# """
import websockets
import asyncio
import json
import logging
from datetime import datetime, timezone
import sqlite3
import threading
import queue
from queue import Queue
import time
import pytz
from urllib.parse import quote
import base64
import os
import struct
from collections import namedtuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('zerodha_ws.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
PING_INTERVAL = 30
# Optimal batch size depends on your tick frequency
OPTIMAL_BATCH_SIZE = 200  # Adjust based on testing
NIFTY_50 = [256265]
BSE_SENSEX = [265]
OTHER_INDICES = [264969,263433,260105,257545,261641,262921,257801,261897,261385,259849,263945,263689,262409,261129,263177,260873,256777,266249,289545,274185,274441,275977,278793,279305,291593,289801,281353,281865]

IndexTick = namedtuple('IndexTick', [
    'token', 'last_price', 'high_price', 'low_price',
    'open_price', 'prev_day_close', 'change', 'exchange_timestamp'
])

# Define the Tick data structure
Tick = namedtuple('Tick', [
    'instrument_token', 'last_price', 'last_quantity', 'avg_price',
    'day_volume', 'buy_quantity', 'sell_quantity', 'open_price', 'high_price',
    'low_price', 'prev_day_close', 'last_trade_timestamp', 'oi', 'oi_day_high',
    'oi_day_low', 'exchange_timestamp', 'depth'
])

DepthEntry = namedtuple('DepthEntry', ['quantity', 'price', 'orders'])
MarketDepth = namedtuple('MarketDepth', ['bids', 'asks'])

import sqlite3
import threading
from queue import Queue
from contextlib import contextmanager

def adapt_datetime(dt: datetime) -> str:
    """Convert datetime to ISO 8601 string with timezone"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def convert_datetime(text: str) -> datetime:
    """Convert ISO 8601 string to datetime"""
    return datetime.fromisoformat(text)

# Register the adapter and converter
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("DATETIME", convert_datetime)

class ThreadSafeDatabase:
    def __init__(self, db_path='ticks.db'):
        self.db_path = db_path
        self.local = threading.local() # This creates thread-local storage
        self.lock = threading.Lock()
        self._initialize_db()

    def _initialize_db(self, force_drop=False):
        """Initialize database schema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Drop old table if exists
            if force_drop:
                cursor.execute("DROP TABLE IF EXISTS market_depth")
                cursor.execute("DROP TABLE IF EXISTS ticks")
            # Enable strict datetime typing
            cursor.execute('PRAGMA strict=ON')
            # Main ticks table with composite primary key
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ticks (
                    instrument_token INTEGER,
                    timestamp DATETIME, -- Will use registered converter
                    last_price REAL,
                    day_volume INTEGER,
                    oi INTEGER,
                    buy_quantity INTEGER,
                    sell_quantity INTEGER,
                    high_price REAL,
                    low_price REAL,
                    open_price REAL,
                    prev_day_close REAL,
                    PRIMARY KEY (instrument_token)
                ) WITHOUT ROWID  -- Better for PK-based lookups
            ''')
            
            # Market depth table with foreign key relationship
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_depth (
                    instrument_token INTEGER,
                    timestamp DATETIME, -- Will use registered converter
                    depth_type TEXT CHECK(depth_type IN ('bid', 'ask')),
                    position INTEGER CHECK(position BETWEEN 1 AND 5),
                    price REAL,
                    quantity INTEGER,
                    orders INTEGER,
                    PRIMARY KEY (instrument_token, depth_type, position),
                    FOREIGN KEY (instrument_token) 
                        REFERENCES ticks(instrument_token)
                        ON DELETE CASCADE
                ) WITHOUT ROWID
            ''')
            
            # Indexes for faster queries
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_depth_main 
                ON market_depth(instrument_token)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp ON ticks(timestamp);
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_instrument ON ticks(instrument_token);
            ''')
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous = NORMAL')
            cursor.execute('PRAGMA cache_size = -70000')  # 70MB cache
            conn.commit()

    def close_all(self):
        """Close all thread connections"""
        if hasattr(self.local, 'conn'):
            self.local.conn.close()

    @contextmanager
    def get_connection(self):
        """Get a thread-local database connection"""
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path, timeout=30)
            self.local.conn.execute('PRAGMA journal_mode=WAL')  # Better for concurrent access
        
        try:
            yield self.local.conn
        except Exception as e:
            self.local.conn.rollback()
            raise e

    def insert_ticks(self, ticks):
        """Thread-safe batch insert with market depth"""
        if not ticks:
            return

        with self.lock, self.get_connection() as conn:
            try:
                # Prepare tick data (tuples are faster than dicts)
                tick_data = [
                    (
                        t['instrument_token'], t['timestamp'], t['last_price'],
                        t['day_volume'], t['oi'], t['buy_quantity'], t['sell_quantity'],
                        t['high_price'], t['low_price'], t['open_price'], t['prev_day_close']
                    )
                    for t in ticks
                ]
                # Batch upsert for ticks
                conn.executemany('''
                    INSERT INTO ticks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instrument_token) DO UPDATE SET
                        timestamp = excluded.timestamp,
                        last_price = excluded.last_price,
                        day_volume = excluded.day_volume,
                        oi = excluded.oi,
                        buy_quantity = excluded.buy_quantity,
                        sell_quantity = excluded.sell_quantity,
                        high_price = excluded.high_price,
                        low_price = excluded.low_price,
                        open_price = excluded.open_price,
                        prev_day_close = excluded.prev_day_close
                ''', tick_data)
                
                # Insert market depth data
                depth_data = []
                for tick in ticks:
                    if 'depth' in tick:
                        ts = tick['timestamp']
                        inst = tick['instrument_token']
                        
                        # Process bids (position 1-5)
                        for i, bid in enumerate(tick['depth']['bid'][:5], 1):
                            depth_data.append((
                                inst, ts, 'bid', i,
                                bid['price'], bid['quantity'], bid['orders']
                            ))
                        
                        # Process asks (position 1-5)
                        for i, ask in enumerate(tick['depth']['ask'][:5], 1):
                            depth_data.append((
                                inst, ts, 'ask', i,
                                ask['price'], ask['quantity'], ask['orders']
                            ))
                
                if depth_data:
                    # Efficient batch upsert using executemany
                    conn.executemany('''
                        INSERT INTO market_depth (
                            instrument_token, timestamp, depth_type,
                            position, price, quantity, orders
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(instrument_token, depth_type, position) 
                        DO UPDATE SET
                            timestamp = excluded.timestamp,
                            price = excluded.price,
                            quantity = excluded.quantity,
                            orders = excluded.orders
                    ''', depth_data)
                
                conn.commit()
                logger.debug(f"Inserted {len(ticks)} ticks.")
            except sqlite3.OperationalError as e:
                logger.error(f"Reinitializing Database. Database Insert error: {str(e)}")
                conn.rollback()
                self.close_all()
                self._initialize_db(force_drop=True)
            except Exception as e:
                logger.error(f"Database insert error: {str(e)}")
                conn.rollback()

class ZerodhaWebSocketParser:
    @staticmethod
    def parse_binary_message(message: bytes) -> list[Tick]:
        """Parse complete binary WebSocket message containing multiple packets"""
        ticks = []
        
        try:
            # First 2 bytes indicate number of packets
            if len(message) < 2:
                return ticks
                
            num_packets = struct.unpack_from('>H', message, 0)[0]
            offset = 2
            
            for _ in range(num_packets):
                if len(message) < offset + 2:
                    break
                    
                # Next 2 bytes indicate packet length
                packet_length = struct.unpack_from('>H', message, offset)[0]
                offset += 2
                
                if len(message) < offset + packet_length:
                    break
                    
                # Extract and parse individual packet
                packet = message[offset:offset+packet_length]
                offset += packet_length
                
                tick = ZerodhaWebSocketParser._parse_single_packet(packet)
                if tick:
                    ticks.append(tick)
                    
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            
        return ticks

    @staticmethod
    def _parse_index_packet(packet: bytes) -> IndexTick | None:
        """Parse index tick packet"""
        fields = struct.unpack('>iiiiiii', packet[:28])
        timestamp = struct.unpack('>i', packet[28:32])[0]
        
        try:
            return IndexTick(
                token=fields[0],
                last_price=fields[1]/100,
                high_price=fields[2]/100,
                low_price=fields[3]/100,
                open_price=fields[4]/100,
                prev_day_close=fields[5]/100,
                change=fields[6]/100,
                exchange_timestamp=timestamp
            )
        except Exception as e:
            logger.error(f"Error parsing Index message: {e}")
        
        return None

    @staticmethod
    def _index_to_regular_tick(index_tick: IndexTick) -> Tick:
        """Convert IndexTick to Tick with maximum performance"""
        return Tick(
            instrument_token=index_tick.token,
            last_price=index_tick.last_price,
            high_price=index_tick.high_price,
            low_price=index_tick.low_price,
            open_price=index_tick.open_price,
            prev_day_close=index_tick.prev_day_close,
            exchange_timestamp=index_tick.exchange_timestamp,
            # Set all unused fields to None explicitly
            last_quantity=None,
            avg_price=None,
            day_volume=None,
            buy_quantity=None,
            sell_quantity=None,
            last_trade_timestamp=None,
            oi=None,
            oi_day_high=None,
            oi_day_low=None,
            depth=None
        )

    @staticmethod
    def _parse_single_packet(packet: bytes) -> Tick | None:
        """Parse a single binary packet into Tick object.

        Right after connecting Zerodha server will send these two text messages first.
        It's only after these two messages are received that we should subscribe to tokens 
        via "subscribe" message and send "mode" message using _subscribe_instruments above.

        {"type": "instruments_meta", "data": {"count": 86481, "etag": "W/\"68907d60-55bf\""}}

        {"type":"app_code","timestamp":"2025-08-04T13:50:28+05:30"}
        
        See https://kite.trade/docs/connect/v3/websocket/ for message structure.

        Always check the type of an incoming WebSocket messages. Market data is always binary and 
        Postbacks and other updates are always text.
        If there is no data to be streamed over an open WebSocket connection, the API will send 
        a 1 byte "heartbeat" every couple seconds to keep the connection alive. This can be safely ignored.
        
        # Binary market data

        WebSocket supports two types of messages, binary and text.

        Quotes delivered via the API are always binary messages. These have to be read as bytes and then type-casted into appropriate quote data structures. On the other hand, all requests you send to the API are JSON messages, and the API may also respond with non-quote, non-binary JSON messages, which are described in the next section.

        For quote subscriptions, instruments are identified with their corresponding numerical instrument_token obtained from the instrument list API.

        # Message structure

        Each binary message (array of 0 to n individual bytes)--or frame in WebSocket terminology--received via the WebSocket is a combination of one or more quote packets for one or more instruments. The message structure is as follows.

        A	The first two bytes ([0 - 2] -- SHORT or int16) represent the number of packets in the message.
        B	The next two bytes ([2 - 4] -- SHORT or int16) represent the length (number of bytes) of the first packet.
        C	The next series of bytes ([4 - 4+B]) is the quote packet.
        D	The next two bytes ([4+B - 4+B+2] -- SHORT or int16) represent the length (number of bytes) of the second packet.
        E	The next series of bytes ([4+B+2 - 4+B+2+D]) is the next quote packet.
        
        # Quote packet structure

        Each individual packet extracted from the message, based on the structure shown in the previous section, can be cast into a data structure as follows. All prices are in paise. For currencies, the int32 price values should be divided by 10000000 to obtain four decimal plaes. For everything else, the price values should be divided by 100.

        Bytes	Type	 
        0 - 4	int32	instrument_token
        4 - 8	int32	Last traded price (If mode is ltp, the packet ends here)
        8 - 12	int32	Last traded quantity
        12 - 16	int32	Average traded price
        16 - 20	int32	Volume traded for the day
        20 - 24	int32	Total buy quantity
        24 - 28	int32	Total sell quantity
        28 - 32	int32	Open price of the day
        32 - 36	int32	High price of the day
        36 - 40	int32	Low price of the day
        40 - 44	int32	Close price (If mode is quote, the packet ends here)
        44 - 48	int32	Last traded timestamp
        48 - 52	int32	Open Interest
        52 - 56	int32	Open Interest Day High
        56 - 60	int32	Open Interest Day Low
        60 - 64	int32	Exchange timestamp
        64 - 184	[]byte	Market depth entries
        
        # Index packet structure

        The packet structure for indices such as NIFTY 50 and SENSEX differ from that of tradeable instruments. They have fewer fields.

        Bytes	Type	 
        0 - 4	int32	Token
        4 - 8	int32	Last traded price
        8 - 12	int32	High of the day
        12 - 16	int32	Low of the day
        16 - 20	int32	Open of the day
        20 - 24	int32	Close of the day
        24 - 28	int32	Price change (If mode is quote, the packet ends here)
        28 - 32	int32	Exchange timestamp
        
        # Market depth structure

        Each market depth entry is a combination of 3 fields, quantity (int32), price (int32), orders (int16) and there is a 2 byte padding at the end (which should be skipped) totalling to 12 bytes. There are ten entries in succession—five [64 - 124] bid entries and five [124 - 184] offer entries.

        Postbacks and non-binary updates
        Apart from binary market data, the WebSocket stream delivers postbacks and other updates in the text mode. These messages are JSON encoded and should be parsed on receipt. For order Postbacks, the payload is contained in the data key and has the same structure described in the Postbacks section.

        # Message structure

        {
            "type": "order",
            "data": {}
        }
        
        # Message types

        type	 
        order	Order Postback. The data field will contain the full order Postback payload
        error	Error responses. The data field contain the error string
        message	Messages and alerts from the broker. The data field will contain the message string
        """
        try:
            # Minimum packet is instrument_token (4) + ltp (4)
            if len(packet) < 8:
                return None

            # See https://github.com/zerodha/pykiteconnect/blob/master/kiteconnect/ticker.py#L719
            # Unpack mandatory fields
            instrument_token, last_price = struct.unpack('>ii', packet[:8])
            last_price /= 100  # Convert from paise to rupees

            # Initialize with default values
            data = {
                'instrument_token': instrument_token,
                'last_price': last_price,
                'last_quantity': None,
                'avg_price': None,
                'day_volume': None,
                'buy_quantity': None,
                'sell_quantity': None,
                'open_price': None,
                'high_price': None,
                'low_price': None,
                'prev_day_close': None,
                'last_trade_timestamp': None,
                'oi': None,
                'oi_day_high': None,
                'oi_day_low': None,
                'exchange_timestamp': None,
                'depth': None
            }

            if instrument_token in NIFTY_50 or instrument_token in BSE_SENSEX or instrument_token in OTHER_INDICES:
                index_tick = ZerodhaWebSocketParser._parse_index_packet(packet)
                if index_tick is not None:
                    return ZerodhaWebSocketParser._index_to_regular_tick(index_tick)

            offset = 8  # Track current position in packet

            # Parse remaining fields based on packet length
            if len(packet) >= 12:
                data['last_quantity'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 16:
                data['avg_price'] = struct.unpack_from('>i', packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 20:
                data['day_volume'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 24:
                data['buy_quantity'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 28:
                data['sell_quantity'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 32:
                data['open_price'] = struct.unpack_from('>i', packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 36:
                data['high_price'] = struct.unpack_from('>i', packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 40:
                data['low_price'] = struct.unpack_from('>i', packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 44:
                data['prev_day_close'] = struct.unpack_from('>i', packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 48:
                data['last_trade_timestamp'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 52:
                data['oi'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 56:
                data['oi_day_high'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 60:
                data['oi_day_low'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            if len(packet) >= 64:
                data['exchange_timestamp'] = struct.unpack_from('>i', packet, offset)[0]
                offset += 4

            # Parse market depth if available (64-184 bytes)
            if len(packet) >= 184:
                depth = {'bid': [], 'ask': []}
                
                # Parse bids (5 entries)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from('>iih', packet, offset)
                        depth['bid'].append(DepthEntry(
                            quantity=quantity,
                            price=price / 100,
                            orders=orders
                        ))
                        offset += 10
                
                # Parse asks (5 entries)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from('>iih', packet, offset)
                        depth['ask'].append(DepthEntry(
                            quantity=quantity,
                            price=price / 100,
                            orders=orders
                        ))
                        offset += 10
                
                data['depth'] = depth

            return Tick(**data)

        except Exception as e:
            logger.error(f"Error parsing packet: {e}")
            return None

class ZerodhaWebSocketClient:
    
    def __init__(self, enctoken, user_id, api_key="kitefront"):
        self.enctoken = enctoken
        self.user_id = user_id
        self.api_key = api_key
        self.ws_url = self._build_websocket_url()
        self.data_queue = Queue(maxsize=10000)
        self.stop_event = threading.Event()
        self.db_conn = ThreadSafeDatabase()
        self.extra_headers = self._build_headers()
        self.last_message_time = time.time()
        self.last_heartbeat = time.time()

    def _build_websocket_url(self):
        """Construct the WebSocket URL with proper parameters"""
        base_params = {
            'api_key': self.api_key,
            'user_id': self.user_id,
            'enctoken': quote(self.enctoken),
            'uid': str(int(time.time() * 1000)),
            'user-agent': 'kite3-web',
            'version': '3.0.0'
        }
        query_string = '&'.join([f"{k}={v}" for k, v in base_params.items()])
        return f"wss://ws.zerodha.com/?{query_string}"

    def _build_headers(self):
        """Generate required WebSocket headers"""
        # Generate random WebSocket key (required for handshake)
        ws_key = base64.b64encode(os.urandom(16)).decode('utf-8')
        
        return {
            'Host': 'ws.zerodha.com',
            'Connection': 'Upgrade',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Upgrade': 'websocket',
            'Origin': 'https://kite.zerodha.com',
            'Sec-WebSocket-Version': '13',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-WebSocket-Key': ws_key,
            'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits'
        }
    
    async def _subscribe_instruments(self, websocket, token_batches, subscribe_all_indices = False):
        """Subscribe to instruments with rate limiting"""
        for batch in token_batches:
            if self.stop_event.is_set():
                break
            
            # Subscribe to Nifty 50 index
            logger.debug(f"Sending NIFTY_50 subscribe and mode messages")
            await websocket.send(json.dumps({"a":"subscribe","v":NIFTY_50}))
            await websocket.send(json.dumps({"a":"mode","v":["full",NIFTY_50]}))

            # Subscribe to BSE Sensex
            logger.debug(f"Sending BSE_SENSEX subscribe and mode messages")
            await websocket.send(json.dumps({"a":"subscribe","v":BSE_SENSEX}))
            await websocket.send(json.dumps({"a":"mode","v":["full",BSE_SENSEX]}))

            if subscribe_all_indices:
                logger.debug(f"Sending OTHER_INDICES subscribe and mode messages")
                await websocket.send(json.dumps({"a":"subscribe","v":OTHER_INDICES}))
                await websocket.send(json.dumps({"a":"mode","v":["full",OTHER_INDICES]}))

            subscribe_msg = {
                "a": "subscribe",
                "v": batch
            }
            # There are three different modes in which quote packets are streamed.
            # modes:	 
            # ltp	    LTP. Packet contains only the last traded price (8 bytes).
            # ltpc	    LTPC. Packet contains only the last traded price and close price (16 bytes).
            # quote	    Quote. Packet contains several fields excluding market depth (44 bytes).
            # full	    Full. Packet contains several fields including market depth (184 bytes).

            mode_msg = {
                "a":"mode",
                "v":["full",batch]
            }
            logger.debug(f"Sending subscribe message: {subscribe_msg}")
            await websocket.send(json.dumps(subscribe_msg))

            logger.debug(f"Sending mode message: {mode_msg}")
            await websocket.send(json.dumps(mode_msg))

            await asyncio.sleep(1)  # Respect rate limits

    async def send_heartbeat(self, websocket):
        # Send heartbeat every 30 seconds
        if time.time() - self.last_heartbeat > PING_INTERVAL:
            await websocket.send(json.dumps({"a": "ping"}))
            self.last_heartbeat = time.time()

    async def _connect_websocket(self):
        """Establish and maintain WebSocket connection"""

        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    self._build_websocket_url(),
                    extra_headers=self._build_headers(),
                    ping_interval=PING_INTERVAL,
                    ping_timeout=10,
                    close_timeout=5,
                    compression=None, #"deflate" # Disable compression for debugging (None instead of deflate)
                    max_size=2**24  # 16MB max message size
                ) as websocket:
                    logger.info("WebSocket connected successfully")
                    
                    # Wait for initial messages
                    initial_messages = []
                    max_wait_counter = 2
                    wait_counter = 0
                    while len(initial_messages) < 2 and wait_counter < max_wait_counter:
                        wait_counter += 1
                        message = await websocket.recv()
                        if isinstance(message, str):
                            data = json.loads(message)
                            if data.get('type') in ['instruments_meta', 'app_code']:
                                initial_messages.append(data)
                                logger.debug(f"Received initial message: {data}")
                        await asyncio.sleep(1)
                    # Subscribe to instruments (example tokens)
                    await self._subscribe_instruments(websocket, token_batches)
                    
                    # Heartbeat every 30 seconds
                    self.last_heartbeat = time.time()
                    
                    # Main message loop
                    await self._message_loop(websocket)
        
            except websockets.exceptions.ConnectionClosedError as e:
                logger.error(f"Connection closed: {e.code} - {e.reason}")
                if e.code == 1000:
                    logger.info("Normal closure, reconnecting...")
                elif e.code == 1011:
                    logger.warning("(unexpected error) keepalive ping timeout, reconnecting...")
                await asyncio.sleep(5)
            except websockets.exceptions.InvalidStatusCode as e:
                logger.error(f"Connection failed with status {e.status_code}")
                if e.status_code == 400:
                    logger.error("Authentication failed. Please check your:")
                    logger.error("- API Key")
                    logger.error("- Access Token")
                    logger.error("- User ID")
                    logger.error("- Token expiration (tokens expire daily)")
                    client.stop()
                    return
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"WebSocket connection error: {str(e)}. Reconnecting in 5 seconds...")
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
                        logger.debug(f"Heartbeat.")
                        continue
                    else:
                        logger.debug(f"Receiving Market Data.")
                        # Process market data
                        ticks = ZerodhaWebSocketParser.parse_binary_message(message)
                        for tick in ticks:
                            self.data_queue.put(tick)
                elif isinstance(message, str):
                    logger.debug(f"Receiving Postbacks or other updates.")
                    # Handle text messages (postbacks and updates)
                    try:
                        data = json.loads(message)
                        self._process_text_message(data)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON message: {message}")
                
            except asyncio.TimeoutError:
                await websocket.ping()
            except Exception as e:
                logger.error(f"Message processing error: {str(e)}")
                break

    def _parse_binary_message(self, message):
        """Parse binary market data messages"""
        ticks = []
        
        try:
            # Read number of packets (first 2 bytes)
            num_packets = struct.unpack_from('>H', message, 0)[0]
            offset = 2
            
            for _ in range(num_packets):
                # Read packet length (next 2 bytes)
                packet_length = struct.unpack_from('>H', message, offset)[0]
                offset += 2
                
                # Extract packet data
                packet = message[offset:offset+packet_length]
                offset += packet_length
                
                # Parse individual packet
                tick = self._parse_binary_packet(packet)
                if tick:
                    ticks.append(tick)
                    
        except Exception as e:
            logger.error(f"Error parsing binary message: {str(e)}")
        
        return ticks

    def _parse_binary_packet(self, packet):
        """Parse individual binary packet with variable length"""
        try:
            # Minimum packet is 8 bytes (instrument_token + ltp)
            if len(packet) < 8:
                logger.warning(f"Packet too short: {len(packet)} bytes")
                return None

            # Unpack common fields (first 8 bytes)
            instrument_token, last_price = struct.unpack('>ii', packet[:8])
            last_price = last_price / 100  # Convert from paise to rupees

            # Initialize tick data with default values
            tick_data = {
                'instrument_token': instrument_token,
                'last_price': last_price,
                'last_quantity': None,
                'avg_price': None,
                'day_volume': None,
                'buy_quantity': None,
                'sell_quantity': None,
                'open_price': None,
                'high_price': None,
                'low_price': None,
                'prev_day_close': None,
                'last_trade_timestamp': None,
                'oi': None,
                'oi_day_high': None,
                'oi_day_low': None,
                'exchange_timestamp': None,
                'depth': None
            }

            # Parse remaining fields based on packet length
            offset = 8
            
            # LTP mode (8-12 bytes)
            if len(packet) >= 12:
                last_quantity = struct.unpack_from('>i', packet, offset)[0]
                tick_data['last_quantity'] = last_quantity
                offset += 4

            # Full mode fields (12+ bytes)
            if len(packet) >= 16:
                avg_price = struct.unpack_from('>i', packet, offset)[0]
                tick_data['avg_price'] = avg_price / 100
                offset += 4

            if len(packet) >= 20:
                volume = struct.unpack_from('>i', packet, offset)[0]
                tick_data['day_volume'] = volume
                offset += 4

            if len(packet) >= 24:
                buy_quantity = struct.unpack_from('>i', packet, offset)[0]
                tick_data['buy_quantity'] = buy_quantity
                offset += 4

            if len(packet) >= 28:
                sell_quantity = struct.unpack_from('>i', packet, offset)[0]
                tick_data['sell_quantity'] = sell_quantity
                offset += 4

            if len(packet) >= 32:
                open_price = struct.unpack_from('>i', packet, offset)[0]
                tick_data['open_price'] = open_price / 100
                offset += 4

            if len(packet) >= 36:
                high_price = struct.unpack_from('>i', packet, offset)[0]
                tick_data['high_price'] = high_price / 100
                offset += 4

            if len(packet) >= 40:
                low_price = struct.unpack_from('>i', packet, offset)[0]
                tick_data['low_price'] = low_price / 100
                offset += 4

            # Quote mode ends here (40-44 bytes)
            if len(packet) >= 44:
                prev_day_close = struct.unpack_from('>i', packet, offset)[0]
                tick_data['prev_day_close'] = prev_day_close / 100
                offset += 4

            if len(packet) >= 48:
                last_trade_timestamp = struct.unpack_from('>i', packet, offset)[0]
                tick_data['last_trade_timestamp'] = last_trade_timestamp
                offset += 4

            if len(packet) >= 52:
                oi = struct.unpack_from('>i', packet, offset)[0]
                tick_data['oi'] = oi
                offset += 4

            if len(packet) >= 56:
                oi_day_high = struct.unpack_from('>i', packet, offset)[0]
                tick_data['oi_day_high'] = oi_day_high
                offset += 4

            if len(packet) >= 60:
                oi_day_low = struct.unpack_from('>i', packet, offset)[0]
                tick_data['oi_day_low'] = oi_day_low
                offset += 4

            if len(packet) >= 64:
                exchange_timestamp = struct.unpack_from('>i', packet, offset)[0]
                tick_data['exchange_timestamp'] = exchange_timestamp
                offset += 4

            # Market depth (64-184 bytes)
            if len(packet) >= 184:
                depth = {'bid': [], 'ask': []}
                
                # Parse 5 bid entries (64-124)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from('>iih', packet, offset)
                        depth['bid'].append({
                            'quantity': quantity,
                            'price': price / 100,
                            'orders': orders
                        })
                        offset += 10
                    else:
                        break
                
                # Parse 5 ask entries (124-184)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from('>iih', packet, offset)
                        depth['ask'].append({
                            'quantity': quantity,
                            'price': price / 100,
                            'orders': orders
                        })
                        offset += 10
                    else:
                        break
                
                tick_data['depth'] = depth

            return tick_data

        except Exception as e:
            logger.error(f"Error parsing packet: {str(e)}")
            return None

    def _process_text_message(self, data):
        """Process non-binary JSON messages"""
        if not isinstance(data, dict):
            return
            
        message_type = data.get('type')
        
        if message_type == 'order':
            self._process_order(data.get('data', {}))
        elif message_type == 'error':
            logger.error(f"Server error: {data.get('data')}")
        elif message_type == 'message':
            logger.info(f"Server message: {data.get('data')}")
        elif message_type == 'instruments_meta':
            logger.debug(f"Instruments metadata update: {data.get('data')}")
        elif message_type == 'app_code':
            logger.debug(f"App code update: {data}")
        else:
            logger.debug(f"Unknown message type: {data}")

    def _process_order(self, order_data):
        """Process order updates"""
        logger.info(f"Order update: {order_data}")
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
                        'instrument_token': tick.instrument_token,
                        'timestamp': datetime.fromtimestamp(tick.exchange_timestamp).replace(tzinfo=pytz.timezone("Asia/Kolkata")), # Explicit IST
                        'last_price': tick.last_price,
                        'day_volume': tick.day_volume,
                        'oi': tick.oi,
                        'buy_quantity': tick.buy_quantity,
                        'sell_quantity': tick.sell_quantity,
                        'high_price': tick.high_price,
                        'low_price': tick.low_price,
                        'open_price': tick.open_price,
                        'prev_day_close': tick.prev_day_close
                    }
                    
                    # Add depth if available
                    if tick.depth:
                        processed['depth'] = {
                            'bid': [
                                {'price': b.price, 'quantity': b.quantity, 'orders': b.orders}
                                for b in tick.depth['bid'][:5]  # Only first 5 levels
                            ],
                            'ask': [
                                {'price': a.price, 'quantity': a.quantity, 'orders': a.orders}
                                for a in tick.depth['ask'][:5]
                            ]
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
                logger.error(f"Error processing ticks: {str(e)}")
        
        # Flush any remaining ticks
        if batch:
            self._flush_to_db(batch)

    async def _refresh_token(self):
        """Refresh expired access token"""
        if time.time() - self.token_timestamp > 86400:  # 24 hours
            logger.info("Refreshing access token")
            # Implement your token refresh logic here
            self.ws_url = self._build_websocket_url()

    async def _connection_monitor(self):
        """Monitor connection health"""
        while not self.stop_event.is_set():
            if not hasattr(self, 'last_message_time'):
                self.last_message_time = time.time()
            
            if time.time() - self.last_message_time > 60:
                logger.warning("No messages received in last 60 seconds")
            await asyncio.sleep(10)

    async def _monitor_performance(self):
        """Monitor system performance"""
        conn = sqlite3.connect('ticks.db', timeout=30)
        while not self.stop_event.is_set():
            # Track processing rate
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM ticks 
                WHERE timestamp >= datetime('now', '-1 minute')
            """)
            ticks_per_minute = cursor.fetchone()[0]
            
            logger.info(
                f"Performance | Queue: {self.data_queue.qsize()} | "
                f"Ticks/min: {ticks_per_minute} | "
                f"DB Lag: {self.data_queue.qsize() / max(1, ticks_per_minute/60):.1f}s"
            )
            
            await asyncio.sleep(60)
        
    def _flush_to_db(self, batch):
        """Bulk insert ticks to database"""
        try:
            self.db_conn.insert_ticks(batch)
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
    
    def start(self):
        """Start WebSocket client and processing threads"""
        logger.info("Starting Zerodha WebSocket client")
        
        # Create event loop for main thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Start WebSocket and other tasks
        self.ws_task = self.loop.create_task(self._connect_websocket())
        self.monitor_task = self.loop.create_task(self._monitor_performance())
        self.conn_monitor_task = self.loop.create_task(self._connection_monitor())
        
        # Start processing thread (still needs to be thread)
        self.processor_thread = threading.Thread(
            target=self._process_ticks,
            daemon=True
        )
        self.processor_thread.start()
        
        # Run the event loop
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Graceful shutdown"""
        logger.info("Stopping Zerodha WebSocket client")
        self.stop_event.set()
        
        # Close all database connections
        self.db_conn.close_all()

        # Cancel all tasks
        for task in [self.ws_task, self.monitor_task, self.conn_monitor_task]:
            if task and not task.done():
                task.cancel()
        
        # Stop the event loop
        if hasattr(self, 'loop'):
            self.loop.stop()
        
        # Wait for processor thread
        if hasattr(self, 'processor_thread'):
            self.processor_thread.join(timeout=5)
        
        self.db_conn.close()
        logger.info("Shutdown complete")

# Usage with your credentials
if __name__ == "__main__":
    tokens = [3329,7707649,5633,6401,912129,3861249,4451329,8705,361473,325121,41729,49409,5166593,54273,60417,67329,70401,2076161,1510401,3848705,4268801,3712257,87297,579329,94977,98049,101121,103425,108033,2714625,2911489,131329,134657,140033,320001,3905025,3812865,160001,163073,524545,177665,1215745,4376065,486657,1946369,6247169]
    # Load instrument tokens from CSV/API
    # with open('instruments.csv') as f:
    #     instruments = pd.read_csv(f)
    #     tokens = instruments['instrument_token'].tolist()

    # Split into batches of 500 (Zerodha's recommended chunk size)
    token_batches = [tokens[i:i+500] for i in range(0, len(tokens), 500)]

    from dotenv import dotenv_values
    local_secrets = dotenv_values(".env.dev")
    
    client = ZerodhaWebSocketClient(
        enctoken=os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token")),
        user_id=os.environ.get("KUSER",local_secrets.get("KUSER","You need your Kite user"))
    )
    
    try:
        client.start()
    except KeyboardInterrupt:
        client.stop()


# Ensure your access_token (enctoken) is fresh (they expire daily)
# To get a fresh access token:

# from kiteconnect import KiteConnect
# kite = KiteConnect(api_key="your_api_key")
# print(kite.generate_session("request_token", "your_api_secret"))

# Testing Steps:
# First verify you can connect using the KiteConnect API
# Ensure your token was generated recently
# Try with just 1-2 instrument tokens initially

# Enable full debug logging:

# logger = logging.getLogger('websockets')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

# Verify token manually:

# import requests
# response = requests.get(
#     "https://api.kite.trade/user/profile",
#     headers={"Authorization": f"enctoken {your_access_token}"}
# )
# print(response.status_code, response.text)

# Check token expiration:

# from datetime import datetime
# token_time = datetime.fromtimestamp(int(your_access_token.split('.')[0]))
# print(f"Token was generated at: {token_time}")