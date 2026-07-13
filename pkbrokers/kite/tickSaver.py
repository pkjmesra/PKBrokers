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
import multiprocessing
import queue
import time
from PKDevTools.classes.log import default_logger

class TickSaver:
    def __init__(self, db_queue, db_conn):
        self.db_queue = db_queue
        self.db_conn = db_conn
        self.stop_event = multiprocessing.Event()
        self.process = None
        self.logger = default_logger()

    def start(self):
        self.process = multiprocessing.Process(target=self._run, daemon=True)
        self.process.start()

    def stop(self):
        self.stop_event.set()
        if self.process:
            self.process.join(timeout=5)

    def _run(self):
        batch = []
        last_flush_time = time.time()
        while not self.stop_event.is_set():
            try:
                try:
                    tick = self.db_queue.get(timeout=1)
                    batch.append(tick)
                except queue.Empty:
                    tick = None

                if tick is None or len(batch) >= 500 or (len(batch) > 0 and time.time() - last_flush_time > 5):
                    if self.db_conn and len(batch) > 0:
                        self.db_conn.insert_ticks(batch)
                    batch = []
                    last_flush_time = time.time()
                
                if tick is None and self.db_queue.empty():
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in TickSaver: {e}", exc_info=True)
