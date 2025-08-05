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
from datetime import datetime, timezone
import sqlite3
import threading
import queue
from queue import Queue
import time
import dateutil
import pytz
from urllib.parse import quote
import base64
import os
import struct
from collections import namedtuple
from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger

# Configure logging
logger = default_logger()
DEFAULT_PATH = Archiver.get_user_data_dir()

PING_INTERVAL = 30
# Optimal batch size depends on your tick frequency
OPTIMAL_BATCH_SIZE = 200  # Adjust based on testing
OPTIMAL_TOKEN_BATCH_SIZE = 500 # Zerodha allows max 500 instruments in one batch
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

class TickMonitor:
    def __init__(self, token_batches=[], db_path: str=os.path.join(DEFAULT_PATH,'ticks.db')):
        self.db_path = db_path
        self.local = threading.local() # This creates thread-local storage
        self.lock = threading.Lock()
        self.last_alert_time = 0
        self.alert_interval = 60  # seconds
        self.subscribed_tokens = token_batches

    async def _get_stale_instruments(self, token_batch: list[int], stale_minutes: int = 1) -> list[int]:
        """
        Find instruments without recent updates
        
        Args:
            token_batch: List of instrument tokens to monitor
            stale_minutes: Threshold in minutes (default: 1)
        
        Returns:
            List of stale instrument tokens
        """
        if not token_batch:
            return []
        
        placeholders = ','.join(['?'] * len(token_batch))
        query = f'''
            SELECT t.instrument_token
            FROM ticks t
            LEFT JOIN instrument_last_update u 
            ON t.instrument_token = u.instrument_token
            WHERE t.instrument_token IN ({placeholders})
            AND (
                u.last_updated IS NULL OR 
                strftime('%s','now') - strftime('%s',u.last_updated) > ? * 60
            )
        '''
        try:
            with sqlite3.connect(os.path.join(DEFAULT_PATH,'ticks.db'), timeout=30) as conn:
                cursor = conn.cursor()
                cursor.execute(query, (*token_batch, stale_minutes))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(e)
        
    async def monitor_stale_updates(self):
        """Continuous monitoring with batch processing"""
        token_batches = self.subscribed_tokens
        stale = await self._check_stale_instruments(token_batches)
        if stale:
            await self._handle_stale_instruments(stale)
            
    async def _check_stale_instruments(self, token_batches: list[list[int]]):
        """Check all batches for stale instruments"""
        stale_instruments = []
        for batch in token_batches:
            stale = await self._get_stale_instruments(batch)
            stale_instruments.extend(stale)
        
        if stale_instruments and time.time() - self.last_alert_time > self.alert_interval:
            logger.warn(f"Stale instruments detected ({len(stale_instruments)}): {stale_instruments}")
            self.last_alert_time = time.time()
            return stale_instruments
        return []
    
    async def _handle_stale_instruments(self, stale):
        logger.warn(f"Following instruments ({len(stale)}) have stale updates:\n{stale}")

    
class ThreadSafeDatabase:
    def __init__(self, db_path=os.path.join(DEFAULT_PATH,'ticks.db')):
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
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS instrument_last_update (
                    instrument_token INTEGER PRIMARY KEY,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS update_timestamp_insert
                AFTER INSERT ON ticks
                FOR EACH ROW
                BEGIN
                    INSERT INTO instrument_last_update (instrument_token, last_updated)
                    VALUES (NEW.instrument_token, CURRENT_TIMESTAMP)
                    ON CONFLICT(instrument_token) DO UPDATE 
                    SET last_updated = CURRENT_TIMESTAMP;
                END;
            ''')
            cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS update_timestamp_update
                AFTER UPDATE ON ticks
                FOR EACH ROW
                BEGIN
                    INSERT INTO instrument_last_update (instrument_token, last_updated)
                    VALUES (NEW.instrument_token, CURRENT_TIMESTAMP)
                    ON CONFLICT(instrument_token) DO UPDATE 
                    SET last_updated = CURRENT_TIMESTAMP;
                END;
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
            logger.debug(f"Tick:{instrument_token}")
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
    
    def __init__(self, enctoken, user_id, api_key="kitefront", token_batches=[]):
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
        self.token_batches = token_batches
        self.token_timestamp = 0
        self.ws_tasks = []
        self.index_subscribed = True

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
    
    def _build_tokens(self):
        import os
        from dotenv import dotenv_values
        from pkbrokers.kite.instruments import KiteInstruments
        local_secrets = dotenv_values(".env.dev")
        API_KEY = "kitefront"
        ACCESS_TOKEN = os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token"))
        self.enctoken = ACCESS_TOKEN
        kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)
        equities_count=kite.get_instrument_count()
        if equities_count == 0:
            kite.sync_instruments(force_fetch=True)
        equities=kite.get_equities(column_names='instrument_token')
        tokens = kite.get_instrument_tokens(equities=equities)
        self.token_batches = [tokens[i:i+OPTIMAL_TOKEN_BATCH_SIZE] for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)]

    async def _subscribe_instruments(self, websocket, token_batches, subscribe_all_indices = False):
        """Subscribe to instruments with rate limiting"""
        if self.stop_event.is_set():
            return
        
        if not self.index_subscribed:
            self.index_subscribed = True
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

        for batch in token_batches:
            if self.stop_event.is_set():
                break

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
            logger.debug(f"Batch size: {len(batch)}. Sending subscribe message: {subscribe_msg}")
            await websocket.send(json.dumps(subscribe_msg))

            logger.debug(f"Sending mode message: {mode_msg}")
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
                async with websockets.connect(
                    self._build_websocket_url(),
                    extra_headers=self._build_headers(),
                    ping_interval=PING_INTERVAL,
                    ping_timeout=10,
                    close_timeout=5,
                    compression="deflate", # Disable compression for debugging (None instead of deflate)
                    max_size=2**17  # 128KB max message size
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
                                self._process_text_message(data=data)
                        await asyncio.sleep(1)
                    # Subscribe to instruments (example tokens)
                    if len(self.token_batches) == 0:
                        self._build_tokens()
                    await self._subscribe_instruments(websocket, self.token_batches if len(token_batch) == 0 else token_batch)
                    
                    # Heartbeat every 30 seconds
                    self.last_heartbeat = time.time()
                    
                    # Main message loop
                    await self._message_loop(websocket)
        
            except websockets.exceptions.ConnectionClosedError as e:
                logger.error(f"Connection closed: {e.code} - {e.reason}")
                if e.code == 1000:
                    logger.info("Normal closure, reconnecting...")
                elif e.code == 1011:
                    logger.warn("(unexpected error) keepalive ping timeout, reconnecting...")
                await asyncio.sleep(5)
            except websockets.exceptions.InvalidStatusCode as e:
                logger.error(f"Connection failed with status {e.status_code}")
                if e.status_code == 400:
                    logger.error("Authentication failed. Please check your:")
                    logger.error("- API Key")
                    logger.error("- Access Token")
                    logger.error("- User ID")
                    logger.error("- Token expiration (tokens expire daily)")
                    self.stop()
                    return
                elif e.status_code in [401, 403]:
                    # the token must have expired
                    await self._refresh_token()

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
                        logger.warn(f"Invalid JSON message: {message}")
                
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
                logger.warn(f"Packet too short: {len(packet)} bytes")
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
            logger.debug(f"Instruments metadata update: {data.get('data')}")
        elif message_type == 'app_code':
            logger.debug(f"App code update: {data}")
            self.token_timestamp = dateutil.parser.isoparse(data.get("timestamp",""))
            self._refresh_token()
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
                        'timestamp': datetime.fromtimestamp(tick.exchange_timestamp, tz=pytz.timezone("Asia/Kolkata")), # Explicit IST
                        'last_price': tick.last_price if tick.last_price is not None else 0,
                        'day_volume': tick.day_volume if tick.day_volume is not None else 0,
                        'oi': tick.oi if tick.oi is not None else 0,
                        'buy_quantity': tick.buy_quantity if tick.buy_quantity is not None else 0,
                        'sell_quantity': tick.sell_quantity if tick.sell_quantity is not None else 0,
                        'high_price': tick.high_price if tick.high_price is not None else 0,
                        'low_price': tick.low_price if tick.low_price is not None else 0,
                        'open_price': tick.open_price if tick.open_price is not None else 0,
                        'prev_day_close': tick.prev_day_close if tick.prev_day_close is not None else 0
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

    async def _refresh_token(self, force=False):
        """Refresh expired access token"""
        if force or (time.time() - self.token_timestamp > 86400):  # 24 hours
            logger.info("Refreshing access token")
            # Implement your token refresh logic here
            from pkbrokers.kite.authenticator import KiteAuthenticator
            auth = KiteAuthenticator()
            encToken = auth.get_enctoken()
            self.enctoken = encToken
            self.ws_url = self._build_websocket_url()

    async def _connection_monitor(self):
        """Monitor connection health"""
        while not self.stop_event.is_set():
            if not hasattr(self, 'last_message_time'):
                self.last_message_time = time.time()
            
            if time.time() - self.last_message_time > 60:
                logger.warn("No messages received in last 60 seconds")
            await asyncio.sleep(10)

    async def _monitor_performance(self):
        """Monitor system performance"""
        conn = sqlite3.connect(os.path.join(DEFAULT_PATH,'ticks.db'), timeout=30)
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

    async def _monitor_stale_instruments(self):
        """Monitor stale instruments"""
        if len(self.token_batches) == 0:
            self._build_tokens()
        tick_monitor = TickMonitor(token_batches = self.token_batches)
        while not self.stop_event.is_set():
            # Track processing rate
            await tick_monitor.monitor_stale_updates()
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
        self.ws_tasks = []
        for token_batch in self.token_batches:
            task = self.loop.create_task(self._connect_websocket([token_batch]))
            self.ws_tasks.append(task)

        self.monitor_task = self.loop.create_task(self._monitor_performance())
        self.monitor_stale_task = self.loop.create_task(self._monitor_stale_instruments())
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
        for task in self.ws_tasks:
            if task and not task.done():
                task.cancel()
        for task in [self.monitor_task, self.conn_monitor_task, self.monitor_stale_task]:
            if task and not task.done():
                task.cancel()
        
        # Stop the event loop
        if hasattr(self, 'loop'):
            self.loop.stop()
        
        # Wait for processor thread
        if hasattr(self, 'processor_thread'):
            self.processor_thread.join(timeout=5)
        
        logger.info("Shutdown complete")

class KiteTokenWatcher:
    def __init__(self, tokens=[]):
        # Split into batches of OPTIMAL_TOKEN_BATCH_SIZE (Zerodha's recommended chunk size)
        self.token_batches = [tokens[i:i+OPTIMAL_TOKEN_BATCH_SIZE] for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)]
    
    def watch(self):
        import os
        from pkbrokers.kite.instruments import KiteInstruments
        from pkbrokers.kite.ticks import ZerodhaWebSocketClient
        from dotenv import dotenv_values
        if len(self.token_batches) == 0:
            local_secrets = dotenv_values(".env.dev")
            API_KEY = "kitefront"
            ACCESS_TOKEN = os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token")),
            kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)
            equities_count=kite.get_instrument_count()
            if equities_count == 0:
                kite.sync_instruments(force_fetch=True)
            equities=kite.get_equities(column_names='instrument_token')
            tokens = kite.get_instrument_tokens(equities=equities)
            tokens = NIFTY_50 + BSE_SENSEX + tokens
            self.token_batches = [tokens[i:i+OPTIMAL_TOKEN_BATCH_SIZE] for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)]

        client = ZerodhaWebSocketClient(
            enctoken=os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token")),
            user_id=os.environ.get("KUSER",local_secrets.get("KUSER","You need your Kite user")),
            token_batches=self.token_batches
        )
        
        try:
            client.start()
        except KeyboardInterrupt:
            client.stop()
"""
# Example usage
if __name__ == "__main__":
    from pkbrokers.kite.instruments import KiteInstruments
    from dotenv import dotenv_values
    local_secrets = dotenv_values(".env.dev")
    API_KEY = "kitefront"
    ACCESS_TOKEN = os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token")),
    kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)
    tokens = kite.get_instrument_tokens(equities=kite.get_equities(column_names='instrument_token'))
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

import os
if __name__ == "__main__":
    from PKDevTools.classes import log
    log.setup_custom_logger(
        "pkscreener",
        log.logging.INFO,
        trace=False,
        log_file_path="PKBrokers-log.txt",
        filter=None,
    )
    os.environ["PKDevTools_Default_Log_Level"] = str(log.logging.INFO)
    from pkbrokers.kite.ticks import KiteTokenWatcher
    watcher = KiteTokenWatcher()
    watcher.watch()
"""