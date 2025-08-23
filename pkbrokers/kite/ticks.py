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

import sqlite3
from collections import namedtuple
from datetime import datetime, timezone

from PKDevTools.classes import Archiver

DEFAULT_PATH = Archiver.get_user_data_dir()

IndexTick = namedtuple(
    "IndexTick",
    [
        "token",
        "last_price",
        "high_price",
        "low_price",
        "open_price",
        "prev_day_close",
        "change",
        "exchange_timestamp",
    ],
)

# Define the Tick data structure
Tick = namedtuple(
    "Tick",
    [
        "instrument_token",
        "last_price",
        "last_quantity",
        "avg_price",
        "day_volume",
        "buy_quantity",
        "sell_quantity",
        "open_price",
        "high_price",
        "low_price",
        "prev_day_close",
        "last_trade_timestamp",
        "oi",
        "oi_day_high",
        "oi_day_low",
        "exchange_timestamp",
        "depth",
    ],
)

DepthEntry = namedtuple("DepthEntry", ["quantity", "price", "orders"])
MarketDepth = namedtuple("MarketDepth", ["bids", "asks"])


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
