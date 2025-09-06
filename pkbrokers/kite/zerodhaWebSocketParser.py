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

import struct

from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKDateUtilities import PKDateUtilities

from pkbrokers.kite.ticks import DepthEntry, IndexTick, Tick

# Configure logging
logger = default_logger()

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


class ZerodhaWebSocketParser:
    @staticmethod
    def parse_binary_message(message: bytes) -> list[Tick]:
        """Parse complete binary WebSocket message containing multiple packets"""
        ticks = []

        try:
            # First 2 bytes indicate number of packets
            if len(message) < 2:
                return ticks

            num_packets = struct.unpack_from(">H", message, 0)[0]
            offset = 2

            for _ in range(num_packets):
                if len(message) < offset + 2:
                    break

                # Next 2 bytes indicate packet length
                packet_length = struct.unpack_from(">H", message, offset)[0]
                offset += 2

                if len(message) < offset + packet_length:
                    break

                # Extract and parse individual packet
                packet = message[offset : offset + packet_length]
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
        try:
            fields = struct.unpack(">iiiiiii", packet[:28])
            timestamp = struct.unpack(">i", packet[28:32])[0]
        except BaseException:
            if not timestamp:
                timestamp = int(PKDateUtilities.currentDateTimestamp())
            pass

        try:
            return IndexTick(
                token=fields[0],
                last_price=fields[1] / 100,
                high_price=fields[2] / 100,
                low_price=fields[3] / 100,
                open_price=fields[4] / 100,
                prev_day_close=fields[5] / 100,
                change=fields[6] / 100,
                exchange_timestamp=timestamp,
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
            exchange_timestamp=index_tick.exchange_timestamp
            or PKDateUtilities.currentDateTimestamp(),
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
            depth=None,
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
            instrument_token, last_price = struct.unpack(">ii", packet[:8])
            last_price /= 100  # Convert from paise to rupees
            logger.debug(f"Tick:{instrument_token}")
            # Initialize with default values
            data = {
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
                "exchange_timestamp": PKDateUtilities.currentDateTimestamp(),
                "depth": None,
            }

            if (
                instrument_token in NIFTY_50
                or instrument_token in BSE_SENSEX
                or instrument_token in OTHER_INDICES
            ):
                index_tick = ZerodhaWebSocketParser._parse_index_packet(packet)
                if index_tick is not None:
                    return ZerodhaWebSocketParser._index_to_regular_tick(index_tick)

            offset = 8  # Track current position in packet

            # Parse remaining fields based on packet length
            if len(packet) >= 12:
                data["last_quantity"] = struct.unpack_from(">i", packet, offset)[0]
                offset += 4

            if len(packet) >= 16:
                data["avg_price"] = struct.unpack_from(">i", packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 20:
                data["day_volume"] = struct.unpack_from(">i", packet, offset)[0]
                offset += 4

            if len(packet) >= 24:
                data["buy_quantity"] = struct.unpack_from(">i", packet, offset)[0]
                offset += 4

            if len(packet) >= 28:
                data["sell_quantity"] = struct.unpack_from(">i", packet, offset)[0]
                offset += 4

            if len(packet) >= 32:
                data["open_price"] = struct.unpack_from(">i", packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 36:
                data["high_price"] = struct.unpack_from(">i", packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 40:
                data["low_price"] = struct.unpack_from(">i", packet, offset)[0] / 100
                offset += 4

            if len(packet) >= 44:
                data["prev_day_close"] = (
                    struct.unpack_from(">i", packet, offset)[0] / 100
                )
                offset += 4

            if len(packet) >= 48:
                data["last_trade_timestamp"] = struct.unpack_from(">i", packet, offset)[
                    0
                ]
                offset += 4

            if len(packet) >= 52:
                data["oi"] = struct.unpack_from(">i", packet, offset)[0]
                offset += 4

            if len(packet) >= 56:
                data["oi_day_high"] = struct.unpack_from(">i", packet, offset)[0]
                offset += 4

            if len(packet) >= 60:
                data["oi_day_low"] = struct.unpack_from(">i", packet, offset)[0]
                offset += 4

            if len(packet) >= 64:
                data["exchange_timestamp"] = struct.unpack_from(">i", packet, offset)[0]
                if data["exchange_timestamp"] is None:
                    data["exchange_timestamp"] = PKDateUtilities.currentDateTimestamp()
                offset += 4

            # Parse market depth if available (64-184 bytes)
            if len(packet) >= 184:
                depth = {"bid": [], "ask": []}

                # Parse bids (5 entries)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from(
                            ">iih", packet, offset
                        )
                        depth["bid"].append(
                            DepthEntry(
                                quantity=quantity, price=price / 100, orders=orders
                            )
                        )
                        offset += 10

                # Parse asks (5 entries)
                for _ in range(5):
                    if len(packet) >= offset + 10:
                        quantity, price, orders = struct.unpack_from(
                            ">iih", packet, offset
                        )
                        depth["ask"].append(
                            DepthEntry(
                                quantity=quantity, price=price / 100, orders=orders
                            )
                        )
                        offset += 10

                data["depth"] = depth

            return Tick(**data)

        except Exception as e:
            logger.error(f"Error parsing packet: {e}")
            return None
