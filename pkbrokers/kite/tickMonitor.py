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
import threading
import time

from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger

DEFAULT_PATH = Archiver.get_user_data_dir()
MAX_ALERT_INTERVAL_SEC = 180


class TickMonitor:
    def __init__(
        self, token_batches=[], db_path: str = os.path.join(DEFAULT_PATH, "ticks.db")
    ):
        self.db_path = db_path
        self.local = threading.local()  # This creates thread-local storage
        self.lock = threading.Lock()
        self.last_alert_time = 0
        self.alert_interval = MAX_ALERT_INTERVAL_SEC  # seconds
        self.subscribed_tokens = token_batches
        # Configure logging
        self.logger = default_logger()

    async def _get_stale_instruments(
        self, token_batch: list[int], stale_minutes: int = 1
    ) -> list[int]:
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

        placeholders = ",".join(["?"] * len(token_batch))
        query = f"""
            SELECT t.instrument_token
            FROM ticks t
            LEFT JOIN instrument_last_update u
            ON t.instrument_token = u.instrument_token
            WHERE t.instrument_token IN ({placeholders})
            AND (
                u.last_updated IS NULL OR
                strftime('%s','now') - strftime('%s',u.last_updated) > ? * 60
            )
        """
        try:
            with sqlite3.connect(
                os.path.join(DEFAULT_PATH, "ticks.db"), timeout=30
            ) as conn:
                cursor = conn.cursor()
                cursor.execute(query, (*token_batch, stale_minutes))
                fetch_all = cursor.fetchall()
                return [row[0] for row in fetch_all]
        except Exception as e:
            self.logger.error(e)
        return []

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

        if (
            stale_instruments
            and time.time() - self.last_alert_time > self.alert_interval
        ):
            self.logger.warn(
                f"Stale instruments detected ({len(stale_instruments)}): {stale_instruments}"
            )
            self.last_alert_time = time.time()
            return stale_instruments
        return []

    async def _handle_stale_instruments(self, stale):
        self.logger.warn(
            f"Following instruments ({len(stale)}) have stale updates:\n{stale}"
        )
