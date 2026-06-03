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
# Already self‑healing
# Component	                | Mechanism	                                   | Recovers from
# WebSocket child processes	| _monitor_processes restarts any dead process | Process crash, deadlock, network drop
#                           | individually	                               | 
# Entire WebSocket client	| KiteTokenWatcher._watchdog (every 60s)       | All processes dead
#                           | restarts if no process is alive	
# Stale batches             |                                              | Individual WebSocket connection stuck but process alive
# (no ticks for 5 min)	    | _monitor_processes restarts that batch	   | 
# Health monitor	        | _trigger_recovery restarts entire client if  | Global tick outage
#                           | too many stale instruments or dead processes | 
# Token expiry	            | _refresh_token and callback try to 
#                           | re‑authenticate	                           | 403 / enctoken errors
# JSON writer	            | Runs in separate process; if it dies, parent | Writer crash
#                           | logs but doesn’t restart it (see below)	   | 
# Database writer	        | Similar to JSON writer – not monitored for   | Writer crash
#                           | crashes	

import json
import multiprocessing
import os
import sys
import threading
import time
import pytz
import fcntl
import pickle
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# Define IST timezone once
IST = pytz.timezone('Asia/Kolkata')

from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Tuple

from PKDevTools.classes import Archiver, log
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger
from PKDevTools.classes.PKJoinableQueue import PKJoinableQueue
from PKDevTools.classes.PKDateUtilities import PKDateUtilities
from PKDevTools.classes.SimplePickler import SimplePickler

from pkbrokers.kite.instruments import KiteInstruments
from pkbrokers.kite.zerodhaWebSocketClient import ZerodhaWebSocketClient
from pkbrokers.kite.inMemoryCandleStore import get_candle_store

# Optimal batch size depends on your tick frequency
OPTIMAL_TOKEN_BATCH_SIZE = 500  # Zerodha allows max 500 instruments in one batch
JSON_WRITER_BATCH_SIZE = 1000  # batch size for processing ticks
OPTIMAL_BATCH_TICK_WAIT_TIME_SEC = 30
DB_PROCESS_SPIN_OFF_WAIT_TIME_SEC = 0.5
JSON_PROCESS_SPIN_OFF_WAIT_TIME_SEC = 1
 # With high tick frequency (e.g., 2000+ instruments, each tick 
 # every few hundred ms), 50k ticks can accumulate in seconds.
 # Large queues increase latency and memory pressure. They mask 
 # backpressure instead of solving the root cause. So, let's
 # keep it to 10k
OPTIMAL_MAX_QUEUE_SIZE = 10000
JSON_SAVE_INTERVAL = 120
DATA_QUEUE_LOG_INTERVAL = 60
STALE_THRESHOLD_SECONDS = 120
TRIGGER_RECOVERY_STALE_PERCENTAGE_THRESHOLD = 10
# Add absolute stale count trigger (e.g., 300 instruments)
TRIGGER_RECOVERY_STALE_ABSOLUTE = 250
RECOVERY_COOLDOWN_SECONDS = 120  # Don't triggers recovery more than once every 2 minutes
MAX_CONSECUTIVE_FAILURES = 5
MIN_INSTRUMENTS_FOR_MONITOR = 500  # Minimum instruments before health monitor activates
SECONDS_BETWEEN_WARNINGS = 30
INSTRUMENT_COUNT_LOG_INTERVAL = 300
HEALTH_CHECK_INTERVAL = 10
CLIENT_WATCHDOG_HEALTH_CHECK_INTERVAL = 60
THREAD_JOIN_TIMEOUT = 10
MAX_RECOVERY_ATTEMPT = 20
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

# macOS fork safety
if sys.platform.startswith("darwin"):
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

def ensure_ist_datetime(dt_value):
        """
        Convert any datetime input to IST timezone-aware datetime
        """
        if dt_value is None:
            return datetime.now(IST)
        
        # If it's already a datetime object
        if hasattr(dt_value, 'tzinfo'):
            # If it has timezone, convert to IST
            if dt_value.tzinfo is not None:
                return dt_value.astimezone(IST)
            else:
                # If naive, assume UTC and convert to IST
                return pytz.UTC.localize(dt_value).astimezone(IST)
        
        # If it's a string, try to parse it
        if isinstance(dt_value, str):
            try:
                # Try parsing with fromisoformat
                dt_parsed = datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
                if dt_parsed.tzinfo is None:
                    dt_parsed = pytz.UTC.localize(dt_parsed)
                return dt_parsed.astimezone(IST)
            except (ValueError, TypeError):
                # If parsing fails, return current IST time
                return datetime.now(IST)
        
        # Default fallback
        return datetime.now(IST)


class TickHealthMonitor:
    """
    Monitors tick reception health across all registered instruments.
    Triggers warnings and recovery actions if no ticks received for too long.

    1. _get_total_instruments() to never return 0
    2. fallback recovery when no instruments are registered
    3. cooldown period to prevent recovery spam
    4. per-WebSocket-process health monitoring
    5. force recovery after prolonged silence
    """
    
    def __init__(self, watcher, stale_threshold_seconds: int = STALE_THRESHOLD_SECONDS):
        """
        Initialize the health monitor.
        
        Args:
            watcher: KiteTokenWatcher instance
            stale_threshold_seconds: Seconds without ticks before triggering action (default 60)
        """
        self.watcher = watcher
        self.stale_threshold = stale_threshold_seconds
        self.last_tick_time = {}  # instrument_token -> last tick timestamp
        self.last_any_tick_time = time.time()
        self.last_any_tick_time_per_process = {}  # websocket_index -> last tick timestamp
        self.logger = default_logger()
        self._monitor_thread = None
        self._stop_event = threading.Event()
        self._recovering = False
        self._recovery_attempts = 0
        self._max_recovery_attempts = MAX_RECOVERY_ATTEMPT
        self._last_recovery_time = 0
        self._recovery_cooldown = RECOVERY_COOLDOWN_SECONDS  # Don't recover more than once per 2 minutes
        self._consecutive_failures = 0
        self._max_consecutive_failures = MAX_CONSECUTIVE_FAILURES
        
        # Tracking for warning intervals (to avoid spam)
        self._last_warning_time = 0
        self._warning_interval = SECONDS_BETWEEN_WARNINGS  # seconds between warnings
        
        # Stats for diagnostics
        self.stats = {
            'start_time': time.time(),
            'recovery_count': 0,
            'warnings_count': 0,
            'last_health_check': 0,
        }
        # Track per-batch health
        self.batch_last_tick_time = {}  # batch_index -> last tick timestamp
        self._batch_lock = threading.Lock()  # Thread-safe updates
        self._last_queue_warning_time = 0
        self._queue_warning_interval = 30
    
    def signal_queue_backlog(self, queue_size):
        """Called when main queue backlog is critical."""
        if time.time() - self._last_queue_warning_time > self._queue_warning_interval:
            self.logger.error(f"🚨 Queue backlog {queue_size} – triggering recovery")
            self._last_queue_warning_time = time.time()
            self._trigger_recovery()

    def start(self):
        """Start the health monitor thread."""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="TickHealthMonitor"
        )
        self._monitor_thread.start()
        self.logger.info(f"TickHealthMonitor started (threshold: {self.stale_threshold}s)")
    
    def stop(self):
        """Stop the health monitor."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)

    def record_tick(self, instrument_token: int, websocket_index: int = None, batch_index: int = None):
        """Record that a tick was received for an instrument."""
        current_time = time.time()
        self.last_tick_time[instrument_token] = current_time
        self.last_any_tick_time = current_time
        
        if websocket_index is not None:
            self.last_any_tick_time_per_process[websocket_index] = current_time
        
        # Track per-batch health
        if batch_index is not None:
            with self._batch_lock:
                self.batch_last_tick_time[batch_index] = current_time
        
        # Reset recovery attempts on successful tick reception
        if self._recovery_attempts > 0:
            self._recovery_attempts = 0
            self._max_recovery_attempts = MAX_RECOVERY_ATTEMPT
            self.logger.debug("✅ Tick received - Recovery attempts reset")
        
        # Reset recovering flag if it was set
        if self._recovering:
            self._recovering = False
    
    def _get_stale_instruments(self) -> list:
        """Get list of instruments that haven't received ticks recently."""
        if not self.last_tick_time:
            return []
        
        current_time = time.time()
        stale = []
        
        for token, last_time in self.last_tick_time.items():
            if current_time - last_time > self.stale_threshold:
                stale.append(token)
        
        return stale
    
    def _get_total_instruments(self) -> int:
        """
        Get total number of registered instruments.
        
        Never returns 0 - if no instruments are registered,
        returns a default positive number to ensure monitor runs.
        """
        # Try token_batches first
        if hasattr(self.watcher, 'token_batches') and self.watcher.token_batches:
            total = sum(len(batch) for batch in self.watcher.token_batches)
            if total > 0:
                return total
        
        # Try shared_stats next
        if hasattr(self.watcher, 'shared_stats') and self.watcher.shared_stats:
            instrument_count = self.watcher.shared_stats.get('instrument_count', 0)
            if instrument_count > 0:
                return instrument_count
        
        # Try last_tick_time keys
        if self.last_tick_time:
            return len(self.last_tick_time)
        
        # If all else fails, return a default positive number
        # This ensures the monitor doesn't skip checks
        # Return 1 so percentage calculation won't divide by zero
        return 1
    
    def _get_dead_websocket_processes(self) -> List[int]:
        """Check which WebSocket processes are dead."""
        dead = []
        
        if not hasattr(self.watcher, 'client') or not self.watcher.client:
            return dead
        
        # Check each WebSocket process
        if hasattr(self.watcher.client, 'ws_processes'):
            for i, proc in enumerate(self.watcher.client.ws_processes):
                if proc and not proc.is_alive():
                    dead.append(i)
        
        # Also check if any process hasn't sent ticks recently
        current_time = time.time()
        for process_idx, last_tick_time in self.last_any_tick_time_per_process.items():
            if current_time - last_tick_time > self.stale_threshold:
                if process_idx not in dead:
                    dead.append(process_idx)
        
        return dead
    
    def _log_warning(self, stale_count: int, total_count: int):
        """Log warning about stale instruments (with rate limiting) - only during market hours."""
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities
        
        # Only log warnings during market hours
        if not PKDateUtilities.isTradingTime():
            # Reset warning timer to avoid logging when market reopens
            self._last_warning_time = time.time()
            return
        
        current_time = time.time()
        if current_time - self._last_warning_time >= self._warning_interval:
            self._last_warning_time = current_time
            self.stats['warnings_count'] += 1
            
            # Get additional context
            is_holiday, holiday_name = PKDateUtilities.isTodayHoliday()
            
            if is_holiday:
                self.logger.debug(f"📅 Holiday ({holiday_name}) - No ticks expected")
                return
            
            # Check for dead WebSocket processes
            dead_processes = self._get_dead_websocket_processes()
            dead_info = f", dead WS processes: {dead_processes}" if dead_processes else ""
            
            self.logger.warning(
                f"⚠️ TICK HEALTH WARNING: {stale_count}/{total_count} instruments have not received ticks "
                f"for {self.stale_threshold} seconds. Last tick received {current_time - self.last_any_tick_time:.1f}s ago{dead_info}."
            )
    
    def _should_attempt_recovery(self) -> bool:
        """
        Determine if recovery should be attempted.
        Use Cooldown period to prevent recovery spam.
        """
        from PKDevTools.classes.PKDateUtilities import PKDateUtilities
        
        # Check cooldown
        if time.time() - self._last_recovery_time < self._recovery_cooldown:
            return False
        
        # Skip on holidays/weekends
        is_holiday, holiday_name = PKDateUtilities.isTodayHoliday()
        if is_holiday:
            self.logger.info(f"📅 Today is a holiday ({holiday_name}) - No recovery needed")
            return False
        current_time = PKDateUtilities.currentDateTime().now()
        # Skip on weekends
        if current_time.now().weekday() >= 5:
            self.logger.info("📅 Weekend - No recovery needed")
            return False
        
        # Allow recovery during market hours OR pre-market (9:00-9:15) OR post-market (15:30-16:00)
        if PKDateUtilities.isTradingTime() or PKDateUtilities.ispreMarketTime():
            return True
        
        # Also allow recovery in the first 15 minutes after market close (to catch any late ticks)
        
        market_close = current_time.now().replace(hour=15, minute=30, second=0, microsecond=0)
        if current_time > market_close and (current_time - market_close).total_seconds() < 900:
            return True
        
        return False
    
    def _trigger_recovery(self):
        """
        Trigger recovery action - restart WebSocket and re-register all instruments.
        
        Enhanced with:
        - Cooldown period
        - Dead process detection
        - Token refresh on authentication errors
        Trigger recovery action - restart WebSocket and re-register all instruments.
        
        Recovery only triggers if:
        1. It's a trading day (not a holiday)
        2. Market is currently open (trading time)
        3. It's a weekday (Monday-Friday)
        
        Recovery Behavior:
        - Stop ALL existing WebSocket processes (clean shutdown)
        - Wait for cleanup
        - Re-initialize the client with the SAME token batches
        - Create the SAME number of new processes (not more)
        """
        if self._recovering:
            return
        
        if not self._should_attempt_recovery():
            return
        
        # Check consecutive failures
        self._consecutive_failures += 1
        if self._consecutive_failures > self._max_consecutive_failures:
            self.logger.error(f"🛑 🛑 🛑 🛑 ❌ {self._consecutive_failures} consecutive failures! Manual intervention may be needed.")
            # Reset counter after logging
            self._consecutive_failures = 0
        
        self.logger.warning("⚠️ ⚠️ Triggering recovery...")
        self._last_recovery_time = time.time()
        self.stats['recovery_count'] += 1
        self._recovery_attempts += 1
        self._recovering = True
        if self._recovery_attempts < self._max_recovery_attempts:
            self.logger.error(f"🛑 🛑 🛑 🛑 ❌ {self._recovery_attempts} recovery attempt failures! Manual intervention may be needed.")
        else:
            sys.exit(1)  # Exit after max recovery attempts to avoid infinite loop
        try:
            # Step 1: Check if token needs refresh
            if self._is_token_expired():
                self.logger.warning("⚠️ Token may be expired, refreshing...")
                self._refresh_token()
            
            # Step 2: Check which WebSocket processes are dead
            dead_processes = self._get_dead_websocket_processes()
            if dead_processes:
                self.logger.warning(f"⚠️ Dead WebSocket processes: {dead_processes}")
            
            # Also restart the consumer thread if it appears dead
            if hasattr(self.watcher, '_processing_thread') and self.watcher._processing_thread:
                if not self.watcher._processing_thread.is_alive():
                    self.logger.warning("Consumer thread dead – restarting")
                    self.watcher._processing_thread = threading.Thread(
                        target=self.watcher._process_ticks, daemon=True, name="TickProcessor"
                    )
                    self.watcher._processing_thread.start()
                    
            # Step 3: Force restart of WebSocket client
            if hasattr(self.watcher, 'client') and self.watcher.client:
                self.logger.warning("Stopping WebSocket client...")
                self.watcher.client.stop_event.set()
                
                # Wait for processes to terminate
                time.sleep(3)
                
                # Force terminate if needed
                if hasattr(self.watcher.client, 'ws_processes'):
                    for i, p in enumerate(self.watcher.client.ws_processes):
                        if p and p.is_alive():
                            self.logger.warning(f"⚠️ Force terminating process {i}")
                            p.terminate()
                            p.join(timeout=3)
            
            # Step 4: Recreate client with same token batches
            if hasattr(self.watcher, 'token_batches') and self.watcher.token_batches:
                self.logger.info(f"Recreating WebSocket client with {len(self.watcher.token_batches)} batches")
                
                from PKDevTools.classes.Environment import PKEnvironment
                
                self.watcher.client = ZerodhaWebSocketClient(
                    enctoken=PKEnvironment().KTOKEN,
                    user_id=self.watcher.user_id if hasattr(self.watcher, 'user_id') else "",
                    token_batches=self.watcher.token_batches,
                    watcher_queue=self.watcher._watcher_queue,
                    db_conn=self.watcher._get_database(),
                    ws_stop_event=self.watcher.ws_stop_event,
                )
                
                # Start the new client
                self.watcher.client.start()
                self.logger.info("✅ WebSocket client restarted successfully")
            else:
                self.logger.error("🛑 🛑 🛑 🛑 Cannot recover - no token batches available")
            
            # Step 5: Reset tracking
            self.last_tick_time.clear()
            self.last_any_tick_time = time.time()
            self.last_any_tick_time_per_process.clear()
            self._recovery_attempts = 0
            
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Recovery failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._recovering = False
    
    def _is_token_expired(self) -> bool:
        """Check if the Kite token might be expired."""
        # Try to make a quick API call to check token validity
        try:
            import requests
            from PKDevTools.classes.Environment import PKEnvironment
            
            token = PKEnvironment().KTOKEN
            if not token or len(token) < 20:
                return True
            
            # Quick validation - check if token looks like a valid enctoken
            # enctoken is typically a long base64-like string
            if not any(c in token for c in ['+', '/', '=']):
                # Token might be invalid format
                return True
            
            return False
        except Exception:
            return False
    
    def _refresh_token(self):
        """Refresh the Kite authentication token."""
        try:
            from pkbrokers.kite.examples.externals import kite_auth
            from PKDevTools.classes.Environment import PKEnvironment
            
            kite_auth()
            new_token = PKEnvironment().KTOKEN
            
            if new_token and len(new_token) > 20:
                # Update environment for child processes
                os.environ["KTOKEN"] = new_token
                self.logger.info("✅ Token refreshed successfully")
            else:
                self.logger.warning("⚠️ Token refresh returned invalid token")
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Token refresh failed: {e}")

    def _get_unhealthy_batches(self, current_time: float) -> List[int]:
        """
        Identify batches that have not received ticks recently.
        
        This is critical because one slow batch can keep the system alive
        while other batches are completely dead.
        
        Returns:
            List of batch indices that are unhealthy
        """
        if not hasattr(self.watcher, 'client') or not self.watcher.client:
            return []
        
        # Try to get batch times from the client first
        if hasattr(self.watcher.client, 'get_batch_last_tick_times'):
            batch_times = self.watcher.client.get_batch_last_tick_times()
            unhealthy = []
            for batch_idx, last_time in batch_times.items():
                if current_time - last_time > self.stale_threshold:
                    unhealthy.append(batch_idx)
            return unhealthy
        
        # Fallback: use local batch tracking
        with self._batch_lock:
            unhealthy = []
            for batch_idx, last_time in self.batch_last_tick_time.items():
                if current_time - last_time > self.stale_threshold:
                    unhealthy.append(batch_idx)
            return unhealthy

    def _monitor_loop(self):
        """Main monitoring loop with enhanced diagnostics - FIXED for slow batches."""
        last_health_check = time.time()
        health_check_interval = HEALTH_CHECK_INTERVAL  # Check every 10 seconds
        last_instrument_count_log = 0
        instrument_count_log_interval = INSTRUMENT_COUNT_LOG_INTERVAL
        
        # Track per-batch health
        self.batch_last_tick_time = {}  # batch_index -> last tick timestamp
        
        while not self._stop_event.is_set():
            try:
                current_time = time.time()
                
                # Log instrument count periodically
                if current_time - last_instrument_count_log > instrument_count_log_interval:
                    total = self._get_total_instruments()
                    self.logger.info(f"Health monitor: {total} instruments registered, {len(self.last_tick_time)} with ticks")
                    last_instrument_count_log = current_time
                
                # Periodic health check
                if current_time - last_health_check >= health_check_interval:
                    last_health_check = current_time
                    self.stats['last_health_check'] = current_time
                    
                    # =============================================================
                    # Skip health checks when market is closed (no ticks expected)
                    # =============================================================
                    from PKDevTools.classes.PKDateUtilities import PKDateUtilities
                    
                    # Check if we should be receiving ticks
                    is_holiday, holiday_name = PKDateUtilities.isTodayHoliday()
                    is_weekend = datetime.fromtimestamp(current_time).weekday() >= 5
                    is_market_open = PKDateUtilities.isTradingTime()
                    
                    # If market is closed, reset timers and skip checks
                    if is_holiday or is_weekend or not is_market_open:
                        # Reset last tick time to avoid false alarms
                        self.last_any_tick_time = current_time
                        self.last_tick_time.clear()
                        with self._batch_lock:
                            self.batch_last_tick_time.clear()
                        if self._recovering:
                            self._recovering = False
                        time.sleep(health_check_interval)
                        continue
                    
                    # Get total instruments (never returns 0)
                    total_instruments = self._get_total_instruments()
                    
                    # ========== NEW: Check batch-level health ==========
                    # Even if ticks are arriving, they might all be from one batch
                    unhealthy_batches = self._get_unhealthy_batches(current_time)
                    
                    if unhealthy_batches:
                        self.logger.error(
                            f"🛑 🛑 🛑 🛑 ❌ {len(unhealthy_batches)} WebSocket batches are unhealthy "
                            f"(no ticks in {self.stale_threshold}s): {unhealthy_batches}"
                        )
                        self._trigger_recovery()
                        continue  # Skip further checks, recover immediately
                    
                    # ========== EXISTING: Check any ticks at all ==========
                    time_since_last_any_tick = current_time - self.last_any_tick_time
                    
                    if time_since_last_any_tick > self.stale_threshold:
                        # NO ticks received for any instrument - critical!
                        dead_processes = self._get_dead_websocket_processes()
                        self.logger.error(
                            f"🛑 🛑 🛑 🛑 ❌ CRITICAL: No ticks received for ANY instrument for {time_since_last_any_tick:.1f}s "
                            f"Dead processes: {dead_processes}"
                        )
                        self._trigger_recovery()
                    else:
                        # ========== EXISTING: Check individual instruments ==========
                        stale_instruments = self._get_stale_instruments()
                        
                        if stale_instruments and total_instruments > 0:
                            stale_percentage = (len(stale_instruments) / total_instruments * 100)
                            self._log_warning(len(stale_instruments), total_instruments)
                            
                            # If more than threshold % of instruments are stale, trigger recovery
                            if (stale_percentage > TRIGGER_RECOVERY_STALE_PERCENTAGE_THRESHOLD or 
                                len(stale_instruments) > TRIGGER_RECOVERY_STALE_ABSOLUTE):
                                self.logger.error(
                                    f"🛑 🛑 🛑 🛑 {stale_percentage:.1f}% of instruments ({len(stale_instruments)} of {total_instruments} instruments) are stale! Triggering recovery..."
                                )
                                self._trigger_recovery()
                            elif stale_percentage > 10:
                                self.logger.debug(f"{stale_percentage:.1f}% instruments stale - monitoring")
                    
                    # Check for dead WebSocket processes
                    dead_processes = self._get_dead_websocket_processes()
                    if dead_processes:
                        self.logger.warning(f"⚠️ Dead WebSocket processes detected: {dead_processes}")
                        self._trigger_recovery()
                        
                time.sleep(1)

            except SystemExit:
                self.stop()
                self.logger.error(f"🛑 🛑 🛑 🛑 🛑 🛑 🛑 🛑 Health monitor stopped. Forced Exit: {e} 🛑 🛑 🛑 🛑")
                raise  # Allow sys.exit() to propagate and stop the monitor
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Health monitor error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5)
        
        self.logger.info("TickHealthMonitor stopped")


class JSONFileWriter:
    """Multiprocessing process to write ticks to JSON file with instrument_token as primary key"""

    def __init__(
        self, json_file_path, max_queue_size=0, log_level=0, writer_id=None
    ):
        # Set spawn context globally
        multiprocessing.set_start_method(
            "spawn" if sys.platform.startswith("darwin") else "spawn", force=True
        )
        self.json_file_path = json_file_path
        self.writer_id = writer_id or f"writer_{os.getpid()}"
        self.lock_file = json_file_path + ".lock"
        self.mp_context = multiprocessing.get_context(
            "spawn" if sys.platform.startswith("darwin") else "spawn"
        )
        self.data_queue = PKJoinableQueue(maxsize=max_queue_size, ctx=self.mp_context)
        self.stop_event = self.mp_context.Event()
        self.process = None
        self.kite_instruments = {}
        self.log_level = log_level
        self._started = False
        self.setupLogger()
        self.logger = default_logger()

    @contextmanager
    def _file_lock(self):
        """
        Context manager for file locking using flock
        Ensures only one process writes to the file at a time
        """
        lock_fd = None
        try:
            # Open lock file for writing (creates if doesn't exist)
            lock_fd = open(self.lock_file, 'w')
            # Acquire exclusive lock (blocks until acquired)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            self.logger.debug(f"Lock acquired by {self.writer_id}")
            yield
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error acquiring lock: {e}")
            raise
        finally:
            if lock_fd:
                try:
                    # Release lock
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    lock_fd.close()
                    self.logger.debug(f"Lock released by {self.writer_id}")
                except Exception as e:
                    self.logger.error(f"🛑 🛑 🛑 🛑 Error releasing lock: {e}")

    def _check_already_running(self):
        """Check if another writer process is already running using PID file"""
        pid_file = self.json_file_path + ".pid"
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                # Check if process with this PID is still running
                try:
                    os.kill(pid, 0)  # Signal 0 just checks if process exists
                    self.logger.warning(f"⚠️ Another JSON writer process (PID: {pid}) is already running!")
                    return True
                except OSError:
                    # Process not running, safe to start
                    self.logger.debug(f"Stale PID file found for PID {pid}, cleaning up")
                    os.unlink(pid_file)
            except (ValueError, IOError) as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Error reading PID file: {e}")
        return False

    def start(self, kite_instruments={}):
        """Start the JSON writer process only once"""
        if self._started:
            self.logger.warning("⚠️ JSON writer already started, ignoring duplicate start")
            return
        
        # Check if another instance is already running
        if self._check_already_running():
            self.logger.error("🛑 🛑 🛑 🛑 Cannot start - another JSON writer instance is running")
            return
        
        self._started = True
        self.process = self.mp_context.Process(target=self._writer_loop)
        self.process.daemon = True
        self.kite_instruments = kite_instruments
        self.process.start()
        
        # Write PID file
        try:
            with open(self.json_file_path + ".pid", 'w') as f:
                f.write(str(self.process.pid))
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error writing PID file: {e}")
        
        self.logger.debug(f"JSON writer started with PID: {self.process.pid}")

    def setupLogger(self):
        if self.log_level > 0:
            os.environ["PKDevTools_Default_Log_Level"] = str(self.log_level)
        log.setup_custom_logger(
            "pkbrokersDB",
            self.log_level,
            trace=False,
            log_file_path="PKBrokers-DBlog.txt",
            filter=None,
        )

    def _deduplicate_data(self, data):
        """
        Deduplicate data by keeping the latest entry for each instrument_token
        based on last_updated timestamp
        """
        clean_data = {}
        
        for token, instrument_data in data.items():
            token_str = str(token)
            
            if token_str in clean_data:
                # Compare last_updated timestamps
                existing = clean_data[token_str].get('last_updated', '1970-01-01T00:00:00+00:00')
                new = instrument_data.get('last_updated', '1970-01-01T00:00:00+00:00')
                
                # Parse and compare timestamps
                try:
                    # Handle ISO format with timezone
                    existing_dt = datetime.fromisoformat(existing.replace('Z', '+00:00'))
                    new_dt = datetime.fromisoformat(new.replace('Z', '+00:00'))
                    
                    if new_dt > existing_dt:
                        clean_data[token_str] = instrument_data
                        self.logger.debug(f"Updated {token_str} with newer data")
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Error comparing timestamps for {token_str}: {e}")
                    # If can't parse, keep the existing one
                    pass
            else:
                clean_data[token_str] = instrument_data
        
        return clean_data

    def _validate_json_structure(self, data):
        """
        Validate that data can be properly serialized to JSON and has no structural issues
        
        Returns:
            tuple: (is_valid, validated_data, error_message)
        """
        try:
            # Test serialization
            json_str = json.dumps(data, default=str)
            
            # Test that it can be parsed back
            parsed = json.loads(json_str)
            
            # Check for data loss during serialization
            if len(parsed) != len(data):
                self.logger.warning(f"⚠️ Data loss detected: {len(data)} -> {len(parsed)}")
                return False, parsed, "Data loss during serialization"
            
            # Validate each instrument has required fields
            required_fields = ['instrument_token', 'trading_symbol', 'ohlcv' ] # 'last_updated'
            for token, instrument in parsed.items():
                missing_fields = [f for f in required_fields if f not in instrument]
                if missing_fields:
                    self.logger.warning(f"⚠️ Instrument {token} missing fields: {missing_fields}")
                    return False, parsed, f"Missing required fields: {missing_fields}"
                
                # Validate ohlcv has required fields
                ohlcv_fields = ['open', 'high', 'low', 'close', 'volume', 'timestamp']
                if 'ohlcv' in instrument:
                    missing_ohlcv = [f for f in ohlcv_fields if f not in instrument['ohlcv']]
                    if missing_ohlcv:
                        self.logger.warning(f"⚠️ Instrument {token} missing OHLCV fields: {missing_ohlcv}")
                        return False, parsed, f"Missing OHLCV fields: {missing_ohlcv}"
            
            return True, parsed, None
            
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 JSON validation failed: {e}")
            return False, None, str(e)

    def _write_atomic(self, data):
        """
        Write data to file atomically using a temporary file
        """
        temp_file = self.json_file_path + ".tmp." + self.writer_id
        
        try:
            # Write to temporary file
            with open(temp_file, "w", encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk
            
            # Verify the temp file was written correctly
            if not os.path.exists(temp_file) or os.path.getsize(temp_file) == 0:
                raise IOError("Temporary file write failed")
            
            # Atomically replace the original file
            os.replace(temp_file, self.json_file_path)
            
            self.logger.debug(f"Successfully wrote {len(data)} instruments to {self.json_file_path}")
            
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error in atomic write: {e}")
            # Clean up temp file if it exists
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            except:
                pass
            raise

    def _save_to_file(self, data, validate=False):
        """
        Save data to JSON file with:
        1. File locking to prevent concurrent writes
        2. Deduplication to ensure unique instrument tokens
        3. JSON validation to ensure data integrity (optional for speed)
        4. Atomic writes to prevent corruption
        """
        temp_file = None
        try:
            # Step 1: Convert defaultdict to regular dict if needed
            if hasattr(data, 'default_factory'):
                data_dict = dict(data)
            else:
                data_dict = data
            
            self.logger.debug(f"Processing {len(data_dict)} raw entries")
            
            # Step 2: Deduplicate data (always do this)
            deduped_data = self._deduplicate_data(data_dict)
            self.logger.debug(f"After deduplication: {len(deduped_data)} unique instruments")
            
            # Step 3: Validate JSON structure (optional for speed)
            if validate:
                is_valid, validated_data, error_msg = self._validate_json_structure(deduped_data)
                
                if not is_valid:
                    self.logger.warning(f"⚠️ Data validation failed: {error_msg}")
                    if validated_data:
                        self.logger.warning("⚠️ Attempting to salvage by using parsed data")
                        validated_data = self._deduplicate_data(validated_data)
                    else:
                        self.logger.warning("⚠️ Data is corrupted beyond repair, skipping save")
                        return
            else:
                # Skip validation, use deduped data directly
                validated_data = deduped_data
            
            # Step 4: Acquire lock and write atomically
            with self._file_lock():
                self.logger.debug("Lock acquired, proceeding with atomic write")
                self._write_atomic(validated_data)
            
            self.logger.info(f"✅ Successfully saved {len(validated_data)} unique instruments to {self.json_file_path}")
            
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 ❌ Error saving JSON file: {e}")
            # Clean up temp file if it exists
            try:
                if temp_file and os.path.exists(temp_file):
                    os.unlink(temp_file)
            except:
                pass

    def _writer_loop(self):
        """Main writer loop running in separate process - OPTIMIZED for speed."""
        self.setupLogger()
        self.logger = default_logger()
        self.logger.debug(f"JSON file writer [{self.writer_id}] started for {self.json_file_path}")
        
        # Load existing data if file exists
        data = {}
        if os.path.exists(self.json_file_path):
            try:
                with open(self.json_file_path, "r") as f:
                    loaded_data = json.load(f)
                    data.update(loaded_data)
                self.logger.info(
                    f"Loaded existing data from {self.json_file_path} with {len(data.keys())} instruments."
                )
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Error loading JSON file: {e}")

        last_save_time = time.time()
        save_interval = JSON_SAVE_INTERVAL
        last_log_time = time.time()
        log_interval = DATA_QUEUE_LOG_INTERVAL
        processed_count = 0
        validation_counter = 0  # For periodic validation

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                # Process MULTIPLE ticks per iteration (batch processing)
                ticks_processed_this_loop = 0
                batch_size = JSON_WRITER_BATCH_SIZE
                temp_batch = []
                
                while ticks_processed_this_loop < batch_size:
                    try:
                        tick_data = self.data_queue.get_nowait()
                        
                        # CRITICAL FIX: Validate tick_data is dict, not tuple
                        if isinstance(tick_data, tuple):
                            self.logger.error(f"🛑 🛑 🛑 🛑 Received tuple instead of dict: {tick_data[:2] if len(tick_data) > 2 else tick_data}")
                            continue  # Skip malformed data
                        
                        if isinstance(tick_data, dict) and 'instrument_token' in tick_data:
                            temp_batch.append(tick_data)
                            processed_count += 1
                            ticks_processed_this_loop += 1
                        else:
                            self.logger.warning(f"⚠️ Skipping invalid tick data type: {type(tick_data)}")
                            
                    except Empty:
                        break
                    except Exception as e:
                        self.logger.error(f"🛑 🛑 🛑 🛑 Error getting tick from queue: {e}")
                        break

                # Process batch
                if temp_batch:
                    for tick_data in temp_batch:
                        self._update_instrument_data(data, tick_data)

                # Save to file periodically
                current_time = time.time()
                if current_time - last_save_time >= save_interval:
                    validation_counter += 1
                    # Run full validation only every 5 saves to save CPU
                    validate = (validation_counter % 5 == 0)
                    self._save_to_file(data, validate=validate)
                    last_save_time = current_time
                    
                    if processed_count > 0:
                        self.logger.debug(
                            f"JSON writer processed {processed_count} ticks, total instruments: {len(data)}"
                        )
                        processed_count = 0
                
                # Log queue status periodically
                if current_time - last_log_time >= log_interval:
                    queue_size = self.data_queue.qsize()
                    if queue_size > 5000:
                        self.logger.warning(f"⚠️ JSON writer input queue backlog: {queue_size} items")
                    elif queue_size > 1000:
                        self.logger.debug(f"JSON writer queue size: {queue_size}")
                    last_log_time = current_time

                # Small sleep to prevent CPU spinning
                time.sleep(0.01)  # REDUCED from 0.05

            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 JSON writer error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)

        # Final save
        self._save_to_file(data, validate=True)
        self.logger.warning("⚠️ JSON writer process stopped")

    def _update_instrument_data(self, data, tick_data):
        """Update instrument data with latest tick information"""
        instrument_token = str(tick_data["instrument_token"])  # Ensure string key

        if instrument_token not in data:
            # Initialize new instrument entry            
            try:
                trading_symbol = "NA"
                trading_symbol = self.kite_instruments[int(instrument_token)].tradingsymbol
            except BaseException:
                if int(instrument_token) in NIFTY_50:
                    trading_symbol = "NIFTY 50"
                elif int(instrument_token) in BSE_SENSEX:
                    trading_symbol = "SENSEX"
                pass
            data[instrument_token] = {
                "instrument_token": int(instrument_token),
                "trading_symbol": trading_symbol,
                "ohlcv": {
                    "open": tick_data["open_price"],
                    "high": tick_data["high_price"],
                    "low": tick_data["low_price"],
                    "close": tick_data["last_price"],
                    "volume": tick_data.get("day_volume", 0),
                    "timestamp": ensure_ist_datetime(tick_data["timestamp"]).isoformat(),
                },
                "prev_day_close": tick_data["prev_day_close"],
                "buy_quantity": tick_data["buy_quantity"],
                "sell_quantity": tick_data["sell_quantity"],
                "oi": tick_data["oi"],
                "market_depth": tick_data.get("depth", {"bid": [], "ask": []}),
                "last_updated": ensure_ist_datetime(tick_data.get("timestamp", datetime.now(IST))).isoformat(),
                "tick_count": 0,
            }

        # Update OHLCV
        current_ohlcv = data[instrument_token]["ohlcv"]
        current_price = tick_data["last_price"]

        # Update high and low
        if current_price > current_ohlcv["high"]:
            current_ohlcv["high"] = current_price
        if current_price < current_ohlcv["low"]:
            current_ohlcv["low"] = current_price

        # Update close and volume
        current_ohlcv["close"] = current_price
        current_ohlcv["volume"] = tick_data.get("day_volume", 0)
        current_ohlcv["timestamp"] = ensure_ist_datetime(tick_data["timestamp"]).isoformat()

        # Update OI, buy_quantity, sell_quantity, prev_day_close
        data[instrument_token]["oi"] = tick_data["oi"]
        data[instrument_token]["buy_quantity"] = tick_data["buy_quantity"]
        data[instrument_token]["sell_quantity"] = tick_data["sell_quantity"]
        data[instrument_token]["prev_day_close"] = tick_data["prev_day_close"]

        # Update market depth (always use latest depth)
        if "depth" in tick_data and tick_data["depth"]:
            data[instrument_token]["market_depth"] = tick_data["depth"]

        # Update metadata
        data[instrument_token]["last_updated"] = ensure_ist_datetime(tick_data.get("timestamp", datetime.now(IST))).isoformat()
        data[instrument_token]["tick_count"] += 1

    def add_batch(self, tick_data_list):
        """Add multiple tick data entries to the write queue as a batch."""
        if not tick_data_list:
            return True
        
        try:
            # Validate each item in batch is a dict
            valid_count = 0
            for tick_data in tick_data_list:
                if isinstance(tick_data, dict) and 'instrument_token' in tick_data:
                    for attempt in range(3):
                        try:
                            self.data_queue.put(tick_data, timeout=0.1)
                            break
                        except Exception:
                            if attempt == 2:
                                self.logger.warning("JSON writer queue full, dropping tick")
                    valid_count += 1
                else:
                    self.logger.warning(f"⚠️ Skipping invalid tick in batch: {type(tick_data)}")
            
            if valid_count == 0:
                self.logger.warning("⚠️ No valid ticks in batch, all skipped")
                return False
                
            return True
        except Exception as e:
            self.logger.warning(f"⚠️ JSON writer queue full or error, dropping batch: {e}")
            return False
        
    def add_tick(self, tick_data):
        """Add tick data to the write queue - auto-convert Tick to dict if needed."""
        # If it's a Tick (namedtuple), convert to dict
        if isinstance(tick_data, tuple) and hasattr(tick_data, '_fields') and 'instrument_token' in tick_data._fields:
            tick_data = {
                "instrument_token": tick_data.instrument_token,
                "last_price": tick_data.last_price,
                "day_volume": tick_data.day_volume,
                "oi": tick_data.oi,
                "buy_quantity": tick_data.buy_quantity,
                "sell_quantity": tick_data.sell_quantity,
                "high_price": tick_data.high_price,
                "low_price": tick_data.low_price,
                "open_price": tick_data.open_price,
                "prev_day_close": tick_data.prev_day_close,
                "timestamp": ensure_ist_datetime(
                    datetime.fromtimestamp(tick_data.last_trade_timestamp) if tick_data.last_trade_timestamp else tick_data.exchange_timestamp
                ).isoformat(),
                "depth": tick_data.depth,
                "websocket_index": getattr(tick_data, 'websocket_index', -1),
                "batch_index": getattr(tick_data, 'batch_index', -1),
            }
        try:
            self.data_queue.put(tick_data, timeout=0.1)
            return True
        except Exception:
            self.logger.warning("⚠️ JSON writer queue full, dropping tick")
            return False

    def stop(self):
        """Stop the JSON writer"""
        self.stop_event.set()
        if self.process and self.process.is_alive():
            self.process.join(timeout=5)
        
        # Clean up PID file
        try:
            pid_file = self.json_file_path + ".pid"
            if os.path.exists(pid_file):
                os.unlink(pid_file)
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error cleaning up PID file: {e}")
        
        self.logger.warning("⚠️ JSON writer stopped")


class KiteTokenWatcher:
    """
    A high-performance tick data watcher and processor for Zerodha Kite Connect API.
    Now includes JSON file writing capability alongside database operations.

    This class manages real-time market data streaming with guaranteed:
    1. Exactly one tick per instrument_token in each batch (latest tick only)
    2. Batch processing every 30 seconds (configurable via OPTIMAL_BATCH_TICK_WAIT_TIME_SEC)
    3. Efficient database operations with proper error handling

    CRITICAL DESIGN FEATURES:
    - Uses dictionary for _tick_batch to ensure only latest tick per instrument is stored
    - Fixed-interval timing logic for consistent 30-second processing cycles
    - Simplified processing pipeline without unnecessary buffering
    - Comprehensive error handling throughout the data flow

    Attributes:
        _watcher_queue (Queue): Queue for receiving raw ticks from WebSocket
        _db_queue (Queue): Queue for processed batches ready for database insertion
        _processing_thread (Thread): Thread for processing raw ticks
        _db_thread (Thread): Thread for database operations
        _shutdown_event (Event): Event signal for graceful shutdown
        token_batches (list): List of token batches for WebSocket subscription
        client (ZerodhaWebSocketClient): WebSocket client instance
        logger (Logger): Logger instance for debugging and monitoring
        _db_instance (ThreadSafeDatabase): Database connection instance
        _tick_batch (dict): Dictionary storing only the latest tick for each instrument
        _next_process_time (datetime): Next scheduled batch processing time

    Example:
        >>> watcher = KiteTokenWatcher(tokens=[256265, 265])
        >>> watcher.watch()  # Starts watching with 30-second batch intervals
        >>> watcher.stop()   # Graceful shutdown
    
    1. token batch loading with multiple fallback sources
    2. WebSocket process health monitoring
    3. shared_stats updates for instrument count
    4. proper error recovery in watch() method
    """

    def __init__(
        self, tokens=[], watcher_queue=None, client=None, json_output_path=None, shared_stats: dict = None
    ):
        """
        Initialize the KiteTokenWatcher instance.

        Args:
            tokens (list): List of instrument tokens to watch. If empty, fetches all equities.
            watcher_queue (Queue): Custom queue for tick data. Creates default if not provided.
            client (ZerodhaWebSocketClient): Pre-configured WebSocket client.
            json_output_path (str): Path for JSON output file. If None, uses default.

        CRITICAL: _tick_batch is a dictionary, not defaultdict(list), ensuring only
        one tick per instrument_token by design (key overwrites on new ticks).
        """
        self._watcher_queue = watcher_queue or Queue(maxsize=OPTIMAL_MAX_QUEUE_SIZE)
        self._db_queue = Queue(maxsize=OPTIMAL_MAX_QUEUE_SIZE)
        self._processing_thread = None
        self._client_watchdog_thread = None
        # self._db_thread = None
        self._shutdown_event = threading.Event()
        self._stop_queue = None
        self._stop_listener_thread = None
        self.log_level = (
            0
            if "PKDevTools_Default_Log_Level" not in os.environ.keys()
            else int(os.environ["PKDevTools_Default_Log_Level"])
        )

        # Split tokens into batches of max 500 (Zerodha limit)
        self.token_batches = [
            tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
            for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
        ]

        self.client = client
        self.logger = default_logger()
        self.pickler = SimplePickler(logger=self.logger)
        self._db_instance = None
        
        # Initialize shared_stats properly
        self.shared_stats = shared_stats if shared_stats is not None else {}
        self.logger.debug(f"KiteTokenWatcher.__init__ received shared_stats: {dict(self.shared_stats) if self.shared_stats else 'None'}")
        
        # Initialize shared_stats with defaults if empty
        if self.shared_stats:
            if 'instrument_count' not in self.shared_stats:
                self.shared_stats['instrument_count'] = 0
            if 'instruments_with_ticks' not in self.shared_stats:
                self.shared_stats['instruments_with_ticks'] = 0
            if 'ticks_processed' not in self.shared_stats:
                self.shared_stats['ticks_processed'] = 0
            if 'uptime_seconds' not in self.shared_stats:
                self.shared_stats['uptime_seconds'] = 0
            if 'candles_created' not in self.shared_stats:
                self.shared_stats['candles_created'] = 0
            if 'candles_completed' not in self.shared_stats:
                self.shared_stats['candles_completed'] = 0
            if 'last_tick_time' not in self.shared_stats:
                self.shared_stats['last_tick_time'] = 0
            if 'start_time' not in self.shared_stats:
                self.shared_stats['start_time'] = time.time()

        # JSON file writer
        self.json_output_path = json_output_path or os.path.join(
            Archiver.get_user_data_dir(), "ticks.json"
        )

        self.json_writer = JSONFileWriter(
            json_file_path=self.json_output_path, log_level=self.log_level
        )
        self.json_writer.setupLogger()

        # CRITICAL: Using dictionary instead of defaultdict(list) ensures only
        # the latest tick for each instrument_token is stored (key overwrite behavior)
        self._tick_batch = {}

        self._next_process_time = None
        self._last_processed_instruments = []
        self._next_process_log_time = None
        # Initialize in-memory candle store for high-performance candle access
        self._candle_store = get_candle_store(shared_stats=self.shared_stats)
        self._kite_instruments = {}
        self.ws_stop_event = None  # Add this for WebSocket stop signal
        # Add health monitor
        self.health_monitor = None
        self.user_id = None  # Store for recovery
        self.executor = ThreadPoolExecutor(max_workers=2)  # one for DB, one for JSON
        self._tick_buffer = []  # local batch buffer
        # self._last_flush_time = time.time()
        # self._buffer_lock = threading.Lock()

    def _process_tick_batch_async(self, tick_list):
        """
        Process a batch of Tick objects in a background thread.
        - Converts ticks to dicts.
        - Updates the in‑memory candle store.
        - Sends data to JSON writer and database.
        """
        if not tick_list:
            return

        try:
            # Convert each tick to dictionary format expected by JSON writer & DB
            processed = [self._tick_to_dict(tick) for tick in tick_list]

            # Update candle store with each tick
            for tick in tick_list:
                self._update_candle_store(tick)   # you need to extract this logic

            # Send entire batch to JSON writer (efficient)
            self.json_writer.add_batch(processed)

            # Insert into database if enabled
            db = self._get_database()
            if db:
                db.insert_ticks(processed)

        except Exception as e:
            self.logger.error(f"Async processing error: {e}")
            
    def set_ws_stop_event(self, event):
        """Set the stop event for WebSocket processes"""
        self.ws_stop_event = event
        self.logger.debug(f"ws_stop_event set to: {event}")
        
    def set_stop_queue(self, stop_queue):
        """
        Set a queue to listen for stop signals from parent process

        Args:
            stop_queue: multiprocessing.Queue instance to listen for stop signals
        """
        self._stop_queue = stop_queue
        self._start_stop_listener()
        # DEBUG CRASH TEST REGION BELOW THIS LINE - USE WITH CAUTION - ONLY FOR TESTING RECOVERY AND RESILIENCE OF THE ORCHESTRATOR
        self._start_command_listener()

    def _start_stop_listener(self):
        """Start a thread to listen for stop signals from the queue"""
        if self._stop_queue is None:
            return

        def listen_for_stop():
            while not self._shutdown_event.is_set():
                try:
                    # Check for stop signal with timeout to avoid blocking indefinitely
                    if self._stop_queue and not self._stop_queue.empty():
                        signal = self._stop_queue.get(timeout=0.1)
                        if signal == "STOP":
                            self.logger.warning(
                                "Received stop signal from launcher/orchestrator"
                            )
                            self.stop()
                            break
                    time.sleep(0.1)
                except Empty:
                    continue
                except Exception as e:
                    self.logger.error(f"🛑 🛑 🛑 🛑 Error in stop listener: {e}")
                    break

        self._stop_listener_thread = threading.Thread(
            target=listen_for_stop, daemon=True, name="StopListener"
        )
        self._stop_listener_thread.start()
        self.logger.debug("Started stop signal listener thread")

    def _watchdog(self):
        """Periodically check WebSocket client health and restart if dead."""
        while not self._shutdown_event.is_set():
            time.sleep(CLIENT_WATCHDOG_HEALTH_CHECK_INTERVAL)
            if self.client and hasattr(self.client, 'ws_processes'):
                alive = any(p.is_alive() for p in self.client.ws_processes)
                if not alive:
                    self.logger.warning("WebSocket client appears dead. Restarting...")
                    self.client.stop()
                    # Recreate and start
                    local_secrets = PKEnvironment().allSecrets
                    self.client = ZerodhaWebSocketClient(
                        enctoken=os.environ.get("KTOKEN", local_secrets.get("KTOKEN", "")),
                        user_id=os.environ.get("KUSER", local_secrets.get("KUSER", "")),
                        token_batches=self.token_batches,
                        watcher_queue=self._watcher_queue,
                        db_conn=self._db_instance,
                        ws_stop_event=self.ws_stop_event,
                    )
                    self.client.start()
            if self._processing_thread and not self._processing_thread.is_alive():
                self.logger.error("Tick processing thread died! Restarting watcher...")
                self.stop()
                self.watch()
                return
            
            if self.json_writer and not self.json_writer.process.is_alive():
                self.logger.error("JSON writer process died – restarting")
                self.json_writer.stop()
                self.json_writer.start(kite_instruments=self._kite_instruments)

    def _update_candle_store(self, tick):
        """
        Update the in‑memory candle store with a single tick.

        Args:
            tick: A Tick object (namedtuple) from the WebSocket.

        Returns:
            bool: True if the candle store was updated successfully, False otherwise.
        """
        try:
            # Get trading symbol from kite_instruments if available
            trading_symbol = ""
            if tick.instrument_token in self._kite_instruments:
                trading_symbol = getattr(
                    self._kite_instruments[tick.instrument_token], 'tradingsymbol', ''
                )

            # Prepare dictionary in the format expected by CandleStore.process_tick()
            tick_for_candle = {
                'instrument_token': tick.instrument_token,
                'last_price': tick.last_price or 0,
                'day_volume': tick.day_volume or 0,
                'oi': tick.oi or 0,
                'exchange_timestamp': tick.exchange_timestamp or time.time(),
                'trading_symbol': trading_symbol,
                'type': 'tick',
                # Additional fields that process_tick might use
                'last_quantity': tick.last_quantity or 0,
                'avg_price': tick.avg_price or 0,
                'open_price': tick.open_price or 0,
                'high_price': tick.high_price or 0,
                'low_price': tick.low_price or 0,
                'prev_day_close': tick.prev_day_close or 0,
                'last_trade_timestamp': tick.last_trade_timestamp or 0,
                'oi_day_high': tick.oi_day_high or 0,
                'oi_day_low': tick.oi_day_low or 0,
            }

            # Process the tick in candle store
            result = self._candle_store.process_tick(tick_for_candle)

            # Update shared_stats if present
            if result and hasattr(self, 'shared_stats') and self.shared_stats is not None:
                self.shared_stats['ticks_processed'] = self._candle_store.stats.get('ticks_processed', 0)
                self.shared_stats['instruments_with_ticks'] = self._candle_store.stats.get('instruments_with_ticks', 0)
                self.shared_stats['instrument_count'] = len(self._candle_store.instruments)

            if not result:
                self.logger.debug(f"Failed to update candle store for {trading_symbol or tick.instrument_token}")

            return result

        except Exception as e:
            self.logger.debug(f"Error updating candle store for token {tick.instrument_token}: {e}")
            return False
        
    def watch(self, test_mode=False):
        """
        Start watching market data for configured tokens.

        This method:
        1. Fetches tokens if not provided during initialization
        2. Initializes WebSocket client if not provided
        3. Starts processing and database threads
        4. Begins WebSocket connection with 30-second batch intervals

        Raises:
            Exception: If WebSocket connection fails or token fetch fails
        """
        max_token_retries = 5
        for attempt in range(max_token_retries):
            token = PKEnvironment().KTOKEN
            if token and len(token) > 10:
                break
            self.logger.warning(f"⚠️ Token missing (attempt {attempt+1}/{max_token_retries}), refreshing...")
            try:
                from pkbrokers.kite.examples.externals import kite_auth
                kite_auth()
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Token refresh failed: {e}")
                time.sleep(30)
        else:
            self.logger.critical("🛑 🛑 🛑 🛑 Cannot obtain KTOKEN after retries. Exiting.")
            return
        local_secrets = PKEnvironment().allSecrets
        self._db_instance = self._get_database()

        # Auto-fetch tokens if none provided
        if len(self.token_batches) == 0:
            tokens = self._fetch_tokens_with_fallback()
            
            if len(tokens) == 0:
                self.logger.error("🛑 🛑 🛑 🛑 ❌ No tokens could be fetched from any source!")
                self.logger.warning("⚠️ Will use fallback indices only...")
                # Use at least indices so we get some data
                tokens = list(set(NIFTY_50 + BSE_SENSEX + OTHER_INDICES))
            
            # Update shared_stats with instrument count
            if self.shared_stats is not None:
                self.shared_stats['instrument_count'] = len(tokens)
                self.logger.debug(f"Updated shared_stats instrument_count = {len(tokens)}")
            
            self.token_batches = [
                tokens[i : i + OPTIMAL_TOKEN_BATCH_SIZE]
                for i in range(0, len(tokens), OPTIMAL_TOKEN_BATCH_SIZE)
            ]
            
            self.logger.info(f"Created {len(self.token_batches)} token batches with {len(tokens)} total instruments")

        # Now start JSON writer with fully populated mapping
        self.json_writer.start(kite_instruments=self._kite_instruments)
        time.sleep(JSON_PROCESS_SPIN_OFF_WAIT_TIME_SEC)
        
        self.logger.debug(
            f"Fetched tokens. Divided into {len(self.token_batches)} batches."
        )

        # Initialize WebSocket client if not provided
        if self.client is None:
            self.logger.debug(f"Creating WebSocket client with ws_stop_event: {self.ws_stop_event}")
            self.client = ZerodhaWebSocketClient(
                enctoken=os.environ.get(
                    "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
                ),
                user_id=os.environ.get(
                    "KUSER", local_secrets.get("KUSER", "You need your Kite user")
                ),
                token_batches=self.token_batches,
                watcher_queue=self._watcher_queue,
                db_conn=self._db_instance,
                ws_stop_event=self.ws_stop_event,
            )

        # Start health monitor after client is initialized
        self.user_id = os.environ.get("KUSER", local_secrets.get("KUSER", ""))
        self.health_monitor = TickHealthMonitor(self, stale_threshold_seconds=STALE_THRESHOLD_SECONDS)
        self.health_monitor.start()
        
        try:
            # # Start database thread
            # self._db_thread = threading.Thread(
            #     target=self._process_db_operations, daemon=True, name="DBProcessor"
            # )
            # self._db_thread.start()
            # time.sleep(DB_PROCESS_SPIN_OFF_WAIT_TIME_SEC)
            
            # Start processing threads
            self._processing_thread = threading.Thread(
                target=self._process_ticks, daemon=True, name="TickProcessor"
            )
            self._processing_thread.start()

            self.logger.debug("Started tick processing and database threads")
            self.client.start()
            # Start client watchdog threads
            self._client_watchdog_thread = threading.Thread(
                target=self._watchdog, daemon=True, name="client_watchdog"
            )
            self._client_watchdog_thread.start()

        except KeyboardInterrupt:
            self.logger.warning("⚠️ Keyboard interrupt received, shutting down...")
            self.stop()
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error in client: {e}")
            self.stop()

    def _fetch_tokens_with_fallback(self) -> List[int]:
        """Fetch instrument tokens with multiple fallback strategies."""
        tokens = []
        local_secrets = PKEnvironment().allSecrets
        API_KEY = "kitefront"
        ACCESS_TOKEN = os.environ.get(
            "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
        )
        kite = KiteInstruments(api_key=API_KEY, access_token=ACCESS_TOKEN)

        try:
            if kite.get_instrument_count() == 0:
                kite.sync_instruments(force_fetch=True)
            instruments = kite.fetch_instruments()
            self._kite_instruments = kite.kite_instruments if len(instruments) > 0 else {}
        except Exception as db_error:
            self.logger.warning(f"⚠️ Database unavailable, using fallback: {db_error}")
            self._kite_instruments = {}
            instruments = []
        
        self.json_writer.start(kite_instruments=self._kite_instruments)
        
        for token, inst in self._kite_instruments.items():
            if hasattr(inst, 'tradingsymbol'):
                self._candle_store.register_instrument(int(token), inst.tradingsymbol)
        time.sleep(JSON_PROCESS_SPIN_OFF_WAIT_TIME_SEC)

        try:
            equities = kite.get_equities(column_names="instrument_token")
            tokens = kite.get_instrument_tokens(equities=equities)
            self.logger.debug(f"Got {len(tokens)} tokens from Turso DB")
            # Register tokens from primary source
            if tokens:
                self._register_loaded_tokens(tokens)
        except Exception as db_error:
            self.logger.warning(f"⚠️ Could not get equities from DB, using fallback: {db_error}")
            tokens = []
        
        if len(tokens) == 0:
            self.logger.warning("⚠️ No tokens from DB, applying fallback strategies...")
            
            # Fallback 0: Use kite_instruments directly
            if self._kite_instruments and len(self._kite_instruments) > 0:
                tokens = [int(token) for token in self._kite_instruments.keys()]
                self.logger.debug(f"Using {len(tokens)} tokens from kite_instruments")
                if tokens:
                    self._register_loaded_tokens(tokens)
            
            # Fallback 1: Fetch NSE equity symbols from PKScreener
            if len(tokens) == 0:
                try:
                    import pandas as pd
                    import requests
                    self.logger.debug("Fetching NSE equity symbols from PKScreener GitHub...")
                    equity_csv_url = "https://raw.githubusercontent.com/pkjmesra/PKScreener/main/results/Indices/EQUITY_L.csv"
                    response = requests.get(equity_csv_url, timeout=30)
                    if response.status_code == 200:
                        from io import StringIO
                        equity_df = pd.read_csv(StringIO(response.text))
                        nse_symbols = equity_df['SYMBOL'].str.strip().tolist()
                        self.logger.debug(f"Fetched {len(nse_symbols)} NSE symbols")
                        
                        kite_url = "https://api.kite.trade/instruments/NSE"
                        kite_response = requests.get(kite_url, timeout=60)
                        if kite_response.status_code == 200:
                            kite_df = pd.read_csv(StringIO(kite_response.text))
                            eq_df = kite_df[(kite_df['segment'] == 'NSE') & (kite_df['tradingsymbol'].isin(nse_symbols))]
                            tokens = eq_df['instrument_token'].tolist()
                            self.logger.debug(f"Mapped {len(tokens)} NSE symbols to tokens")
                            for _, row in eq_df.iterrows():
                                self._candle_store.register_instrument(int(row['instrument_token']), row.get('tradingsymbol', str(row['instrument_token'])))
                            if tokens:
                                self._register_loaded_tokens(tokens)
                except Exception as pkscreener_error:
                    self.logger.warning(f"⚠️ PKScreener fallback failed: {pkscreener_error}")
            
            # Fallback 2: Use only indices as last resort
            if len(tokens) == 0:
                tokens = list(set(NIFTY_50 + BSE_SENSEX + OTHER_INDICES))
                self.logger.warning(f"⚠️ Using fallback indices only: {len(tokens)} instruments")
                self._register_loaded_tokens(tokens)
        
        # Always include indices
        tokens = list(set(NIFTY_50 + BSE_SENSEX + OTHER_INDICES + tokens))
        
        if len(tokens) <= len(NIFTY_50 + BSE_SENSEX + OTHER_INDICES):
            self.logger.warning(f"⚠️ Only {len(tokens)} index tokens available. Ticks will be limited to indices only.")
        
        return tokens

    def _get_database(self):
        """
        Get or create the thread-safe database instance.

        Returns:
            ThreadSafeDatabase: Database instance for tick storage

        Note: Uses lazy initialization to avoid unnecessary database connections
        """
        if PKEnvironment().DB_TICKS and int(PKEnvironment().DB_TICKS) > 0:
            if self._db_instance is None:
                from pkbrokers.kite.threadSafeDatabase import ThreadSafeDatabase

                self._db_instance = ThreadSafeDatabase()
            return self._db_instance
        return None

    def _tick_to_dict(self, tick):
        """Convert a Tick object to a dictionary for JSON writer."""
        return {
            "instrument_token": tick.instrument_token,
            "timestamp": ensure_ist_datetime(
                datetime.fromtimestamp(tick.last_trade_timestamp) if tick.last_trade_timestamp else tick.exchange_timestamp
            ).isoformat(),
            "last_price": tick.last_price or 0,
            "day_volume": tick.day_volume or 0,
            "oi": tick.oi or 0,
            "buy_quantity": tick.buy_quantity or 0,
            "sell_quantity": tick.sell_quantity or 0,
            "high_price": tick.high_price or 0,
            "low_price": tick.low_price or 0,
            "open_price": tick.open_price or 0,
            "prev_day_close": tick.prev_day_close or 0,
            "depth": self._extract_depth(tick),
            "websocket_index": getattr(tick, 'websocket_index', -1),
            "batch_index": getattr(tick, 'batch_index', -1),
        }
    
    def _flush_to_json_writer(self):
        """Flush pending ticks to JSON writer with monitoring."""
        if not hasattr(self, '_tick_batch') or not self._tick_batch:
            return
        
        try:
            start_time = time.time()
            
            # Send entire batch of ticks to JSON writer
            tick_dicts = [self._tick_to_dict(tick) for tick in self._tick_batch.values()]
            self.json_writer.add_batch(tick_dicts)   # ensure add_batch puts all at once
            
            # Check if JSON writer is falling behind
            if hasattr(self.json_writer, 'data_queue'):
                queue_size = self.json_writer.data_queue.qsize()
                if queue_size > 5000:
                    self.logger.warning(f"⚠️ ⚠️ JSON writer queue backlog: {queue_size} items - writer may be a bottleneck!")
            
            elapsed = time.time() - start_time
            if elapsed > 0.5:
                self.logger.warning(f"⚠️ JSON flush took {elapsed:.2f}s for {len(self._tick_batch)} ticks")
            
            self._tick_batch.clear()
            
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error flushing to JSON writer: {e}")
            
    def _process_tick_batch(self, tick_batch):
        """
        Process a batch of ticks for all instruments with full OHLCV and depth processing.

        Args:
            tick_batch (dict): Dictionary mapping instrument tokens to their latest ticks

        CRITICAL: This method expects each instrument_token to have exactly one tick
        (the latest), ensuring no duplicates in the final database insert.
        """
        if not tick_batch:
            return

        processed_batch = []
        total_instruments = len(tick_batch)
        self.logger.debug(
            f"Processing batch with {total_instruments} unique instruments"
        )
        # Batch for JSON writer - collect all ticks first
        json_batch = []

        for instrument_token, ticks in tick_batch.items():
            if not ticks:
                continue

            # CRITICAL: We only have one tick per instrument (the latest)
            latest_tick = ticks[0]  # Single tick in list format
            timestamp = datetime.fromtimestamp(latest_tick.last_trade_timestamp) if latest_tick.last_trade_timestamp else latest_tick.exchange_timestamp

            processed = self._tick_to_dict(latest_tick)
            processed_batch.append(processed)
            # Add to JSON batch instead of sending individually
            json_batch.append(processed)

        # NEW: Send ENTIRE BATCH to JSON writer at once
        if json_batch:
            try:
                # Send each tick in batch to JSON writer (still individual but now batched by size)
                # Option A: Send one by one (original behavior but with batching at this level)
                # for tick_data in json_batch:
                #     self.json_writer.add_tick(tick_data)
                
                # Option B: Send as a single batch (requires JSON writer modification)
                self.json_writer.add_batch(json_batch)
                
                # Log queue status periodically
                if hasattr(self.json_writer, 'data_queue'):
                    queue_size = self.json_writer.data_queue.qsize()
                    if queue_size > 10000:
                        self.logger.warning(f"⚠️ ⚠️ JSON writer queue backlog: {queue_size} items")
                    elif queue_size > 5000:
                        self.logger.debug(f"JSON writer queue size: {queue_size}")
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Error sending to JSON writer: {e}")
            
            # FIX: Update in-memory candle store with properly formatted tick data
            try:
                trading_symbol = ""
                if instrument_token in self._kite_instruments:
                    trading_symbol = getattr(self._kite_instruments[instrument_token], 'tradingsymbol', '')
                
                # IMPORTANT: Create dictionary with EXACT keys expected by process_tick()
                tick_for_candle = {
                    'instrument_token': latest_tick.instrument_token,
                    'last_price': latest_tick.last_price or 0,
                    # Use day_volume (cumulative) for daily candles
                    'day_volume': latest_tick.day_volume or 0,
                    'oi': latest_tick.oi or 0,
                    # Use exchange_timestamp for candle aggregation
                    'exchange_timestamp': latest_tick.exchange_timestamp or time.time(),
                    'trading_symbol': trading_symbol,
                    'type': 'tick',
                    # Add these additional fields that process_tick might expect
                    'last_quantity': latest_tick.last_quantity or 0,
                    'avg_price': latest_tick.avg_price or 0,
                    'open_price': latest_tick.open_price or 0,
                    'high_price': latest_tick.high_price or 0,
                    'low_price': latest_tick.low_price or 0,
                    'prev_day_close': latest_tick.prev_day_close or 0,
                    'last_trade_timestamp': latest_tick.last_trade_timestamp or 0,
                    'oi_day_high': latest_tick.oi_day_high or 0,
                    'oi_day_low': latest_tick.oi_day_low or 0,
                }
                
                # Process the tick in candle store
                if self._candle_store.process_tick(tick_for_candle):
                    self.logger.debug(f"Updated candle store for {trading_symbol or instrument_token}")
                else:
                    self.logger.debug(f"Failed to update candle store for {trading_symbol or instrument_token}")
                    
                # Update shared_stats via candle store
                if hasattr(self, 'shared_stats') and self.shared_stats is not None:
                    self.shared_stats['ticks_processed'] = self._candle_store.stats.get('ticks_processed', 0)
                    self.shared_stats['instruments_with_ticks'] = self._candle_store.stats.get('instruments_with_ticks', 0)
                    self.shared_stats['instrument_count'] = len(self._candle_store.instruments)
                    
            except Exception as e:
                self.logger.debug(f"Error updating candle store: {e}")

        # Insert into database
        try:
            db = self._get_database()
            if db:
                db.insert_ticks(processed_batch)
                self.logger.debug(
                    f"Successfully added {len(processed_batch)} records to database queue"
                )
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error inserting to database: {e}")

    def _flush_candles_to_db(self):
        """Send completed 1‑minute candles from candle store to DB."""
        if not self._db_instance:
            return
        candles = self._candle_store.get_completed_candles('1m')
        if candles:
            self._db_instance.insert_candles_batch(candles)
            self.logger.debug(f"Flushed {len(candles)} candles to DB")
            
    def _process_ticks(self):
        """
        Non‑blocking consumer with batching and thread‑pool offload.
        
        - Collects ticks until BATCH_SIZE or BATCH_INTERVAL is reached.
        - Offloads the heavy processing (DB, JSON, candles) to a background thread pool.
        - Monitors queue depth and signals health monitor if backlog becomes critical.
        """
        from pkbrokers.kite.ticks import Tick
        from queue import Empty

        BATCH_SIZE = 500          # process 500 ticks before flushing
        BATCH_INTERVAL = 1.0      # or every second
        _tick_buffer = []         # local buffer for ticks (list of Tick objects)

        while not self._shutdown_event.is_set():
            try:
                start_time = time.time()
                # Collect ticks until buffer is full or interval elapsed
                while len(_tick_buffer) < BATCH_SIZE and \
                    (time.time() - start_time) < BATCH_INTERVAL:
                    try:
                        tick = self._watcher_queue.get(timeout=0.05)
                    except Empty:
                        break

                    if tick and isinstance(tick, Tick):
                        _tick_buffer.append(tick)
                        # Record tick for health monitor immediately
                        if self.health_monitor:
                            self.health_monitor.record_tick(
                                tick.instrument_token,
                                getattr(tick, 'websocket_index', None),
                                getattr(tick, 'batch_index', None)
                            )
                        self._watcher_queue.task_done()

                # If buffer has data, flush asynchronously
                if _tick_buffer:
                    batch_to_process = _tick_buffer.copy()
                    _tick_buffer.clear()
                    # Offload to thread pool – does not block the main loop
                    try:
                        self.executor.submit(self._process_tick_batch_async, batch_to_process)
                    except RuntimeError as e:
                        if "cannot schedule new futures" in str(e):
                            self.logger.debug("Executor already shut down – ignoring submit")
                        else:
                            raise

                # Optional: monitor queue depth for health
                qsize = self._watcher_queue.qsize()
                if qsize > 20000 and self.health_monitor:
                    self.health_monitor.signal_queue_backlog(qsize)

                # Small sleep to prevent CPU spinning
                time.sleep(0.001)

            except KeyboardInterrupt:
                self.logger.warning("⚠️ Keyboard interrupt received in processing thread")
                break
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Unexpected error in tick processing: {e}")
                continue

        # Final flush of any remaining ticks before shutdown
        if _tick_buffer:
            try:
                self.executor.submit(self._process_tick_batch_async, _tick_buffer)
            except RuntimeError as e:
                if "cannot schedule new futures" in str(e):
                    self.logger.debug("Executor already shut down – ignoring final flush")
                else:
                    raise
        self.executor.shutdown(wait=True)
        # self._cleanup_processing()
        
    def _cleanup_processing(self):
        """Handle graceful shutdown with proper cleanup of remaining data."""
        if self._tick_batch:
            batch_dict = {token: [tick] for token, tick in self._tick_batch.items()}
            self._db_queue.put(batch_dict)
            self.logger.info(
                f"Processed {len(batch_dict)} final instruments on shutdown"
            )

        self._db_queue.put(None)  # Signal database thread to exit
        self.logger.warning("⚠️ Exiting tick processing thread")

    def _process_db_operations(self):
        """
        Dedicated database thread that processes batches from the queue.

        This thread:
        - Processes batches immediately as they arrive
        - Uses the full _process_tick_batch method for comprehensive processing
        - Includes robust error handling with fallback mechanisms
        - Ensures no batch loss during processing
        """
        self.logger.debug("Database processing thread started")

        while not self._shutdown_event.is_set():
            try:
                # Get batch with reasonable timeout
                batch = self._db_queue.get(timeout=2)

                if batch is None:  # Shutdown signal
                    self.logger.debug("Received shutdown signal in DB thread")
                    break

                # Process batch immediately using full processing method
                try:
                    self._process_tick_batch(batch)
                except Exception as e:
                    self.logger.error(f"🛑 🛑 🛑 🛑 Full processing failed, using fallback: {e}")
                    # Fallback to simple processing if full processing fails
                    self._process_batch_fallback(batch)

            except Empty:
                # Normal timeout, continue waiting
                continue
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Database thread error: {e}")
                continue

        self.logger.warning("⚠️ Exiting database processing thread")

    def _process_batch_fallback(self, tick_batch):
        """
        Fallback processing method when full processing fails.

        Args:
            tick_batch (dict): Batch to process with simplified logic
        """
        try:
            processed_batch = self._prepare_batch_for_insertion(tick_batch)
            if processed_batch:
                db = self._get_database()
                if db:
                    db.insert_ticks(processed_batch)
                    self.logger.info(f"Fallback inserted {len(processed_batch)} records")
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Fallback processing also failed: {e}")

    def _prepare_batch_for_insertion(self, tick_batch):
        """
        Simplified batch preparation for database insertion.

        Args:
            tick_batch (dict): Dictionary of instrument tokens to ticks

        Returns:
            list: Processed data ready for database insertion

        Note: This is a fallback method and doesn't include full OHLCV processing
        """
        processed_batch = []

        for instrument_token, ticks in tick_batch.items():
            if not ticks:
                continue

            latest_tick = ticks[0]
            timestamp = datetime.fromtimestamp(latest_tick.exchange_timestamp)

            processed = {
                "instrument_token": latest_tick.instrument_token,
                "timestamp": ensure_ist_datetime(timestamp),
                "last_price": latest_tick.last_price or 0,
                "day_volume": latest_tick.day_volume or 0,
                "oi": latest_tick.oi or 0,
                "buy_quantity": latest_tick.buy_quantity or 0,
                "sell_quantity": latest_tick.sell_quantity or 0,
                "high_price": latest_tick.high_price or 0,
                "low_price": latest_tick.low_price or 0,
                "open_price": latest_tick.open_price or 0,
                "prev_day_close": latest_tick.prev_day_close or 0,
                "depth": self._extract_depth(latest_tick)
                if hasattr(latest_tick, "depth")
                else {},
            }
            processed_batch.append(processed)

        return processed_batch

    def _extract_depth(self, tick):
        """
        Extract market depth data from a tick.

        Args:
            tick: The tick object containing depth information

        Returns:
            dict: Market depth data with bid/ask information
        """
        depth = {"bid": [], "ask": []}

        if not hasattr(tick, "depth"):
            return depth

        for i in range(1, 6):
            # Process bids
            bid_price = getattr(tick.depth, f"buy_{i}_price", 0)
            bid_quantity = getattr(tick.depth, f"buy_{i}_quantity", 0)
            bid_orders = getattr(tick.depth, f"buy_{i}_orders", 0)

            if bid_price and bid_quantity:
                depth["bid"].append(
                    {
                        "price": bid_price,
                        "quantity": bid_quantity,
                        "orders": bid_orders,
                    }
                )

            # Process asks
            ask_price = getattr(tick.depth, f"sell_{i}_price", 0)
            ask_quantity = getattr(tick.depth, f"sell_{i}_quantity", 0)
            ask_orders = getattr(tick.depth, f"sell_{i}_orders", 0)

            if ask_price and ask_quantity:
                depth["ask"].append(
                    {
                        "price": ask_price,
                        "quantity": ask_quantity,
                        "orders": ask_orders,
                    }
                )

        return depth

    def _atomic_pickle_dump(self, data: Dict, filepath: str):
        """Atomically write pickle file using temporary file."""
        import tempfile
        import shutil
        
        temp_fd, temp_path = tempfile.mkstemp(
            suffix='.pkl.tmp',
            prefix=os.path.basename(filepath) + '.',
            dir=os.path.dirname(filepath)
        )
        
        try:
            with os.fdopen(temp_fd, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                f.flush()
                os.fsync(f.fileno())
            
            # Verify the temp file is valid
            with open(temp_path, 'rb') as f:
                test_data = pickle.load(f)
            
            # Replace original with temp file
            shutil.move(temp_path, filepath)
            self.logger.info(f"Atomically saved pickle to {filepath}")
            
        except Exception as e:
            os.unlink(temp_path)
            self.logger.error(f"🛑 🛑 🛑 🛑 Failed to atomically save pickle: {e}")
            raise
    
    def _register_loaded_tokens(self, tokens: List[int]):
        """Helper method to update shared_stats and candle_store when tokens are loaded."""
        if not tokens:
            return
        
        # Update shared_stats
        if self.shared_stats is not None:
            self.shared_stats['instrument_count'] = len(tokens)
            self.logger.info(f"Updated shared_stats: instrument_count = {len(tokens)}")
        
        # Register instruments with candle store
        for token in tokens:
            symbol = "N/A"
            if self._kite_instruments and token in self._kite_instruments:
                symbol = getattr(self._kite_instruments[token], 'tradingsymbol', 'N/A')
            self._candle_store.register_instrument(int(token), symbol)
            # Also update the health monitor's tracking to prevent early false alarms
            if hasattr(self, 'health_monitor') and self.health_monitor:
                self.health_monitor.last_tick_time[int(token)] = time.time()
                self.health_monitor.last_any_tick_time = time.time()
                
    def get_candles(
        self,
        symbol: str = None,
        instrument_token: int = None,
        interval: str = '5m',
        count: int = 50,
    ):
        """
        Get candles for an instrument from the in-memory store.
        
        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            instrument_token: Instrument token (alternative to symbol)
            interval: Candle interval ('1m', '2m', '3m', '4m', '5m', '10m', '15m', '30m', '60m', 'day')
            count: Number of candles to return
            
        Returns:
            List of candle dictionaries
        """
        return self._candle_store.get_candles(
            instrument_token=instrument_token,
            trading_symbol=symbol,
            interval=interval,
            count=count,
        )
    
    def get_candles_df(
        self,
        symbol: str = None,
        instrument_token: int = None,
        interval: str = '5m',
        count: int = 50,
    ):
        """
        Get candles as a pandas DataFrame.
        
        Args:
            symbol: Trading symbol
            instrument_token: Instrument token
            interval: Candle interval
            count: Number of candles
            
        Returns:
            DataFrame with OHLCV columns
        """
        return self._candle_store.get_ohlcv_dataframe(
            instrument_token=instrument_token,
            trading_symbol=symbol,
            interval=interval,
            count=count,
        )
    
    def get_day_ohlcv(self, symbol: str = None, instrument_token: int = None):
        """Get today's OHLCV data for an instrument."""
        return self._candle_store.get_day_ohlcv(
            instrument_token=instrument_token,
            trading_symbol=symbol,
        )
    
    def get_all_day_ohlcv(self):
        """Get today's OHLCV for all instruments."""
        return self._candle_store.get_all_instruments_ohlcv(interval='day')
    
    def get_candle_store_stats(self):
        """Get statistics from the candle store."""
        return self._candle_store.get_stats()
    
    def export_candle_store(self, pickle_path: str = None, json_path: str = None):
        """
        Export candle store data to files.
        
        Args:
            pickle_path: Path for pickle file (optional)
            json_path: Path for ticks.json file (optional)
        """
        if json_path:
            self._candle_store.save_ticks_json(json_path)
        elif json_path is None:
            self._candle_store.save_ticks_json()
        
        if pickle_path:
            data = self._candle_store.export_to_pickle_format()
            import pickle
            with open(pickle_path, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            self.logger.info(f"Exported candle store to {pickle_path}")

    def stop(self):
        """
        Graceful shutdown of all components.

        This method ensures:
        - Proper signaling to all threads
        - Cleanup of remaining data
        - Timeout-based thread termination
        - Resource cleanup
        """
        self.logger.info("Initiating graceful shutdown...")
        
        # Stop health monitor
        if self.health_monitor:
            self.health_monitor.stop()

        # Signal WebSocket processes to stop if we have the event
        if self.ws_stop_event:
            self.logger.info("Signaling WebSocket processes to stop...")
            self.ws_stop_event.set()
        
        # Stop WebSocket client first to stop receiving new ticks
        if self.client:
            try:
                self.client.stop()
                self.logger.warning("⚠️ WebSocket client stopped")
            except Exception as e:
                self.logger.error(f"🛑 🛑 🛑 🛑 Error stopping client: {e}")
        
        # Save candle store data before shutdown
        try:
            self._candle_store.save_ticks_json()
            self.logger.info("Saved candle store to ticks.json")
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error saving candle store: {e}")
        
        # Export pkl files with candle data
        try:
            from pkbrokers.bot.dataSharingManager import DataSharingManager
            from datetime import datetime
            import os
            import shutil
            
            # Use results/Data directory relative to cwd for workflow compatibility
            results_dir = os.path.join(os.getcwd(), "results", "Data")
            os.makedirs(results_dir, exist_ok=True)
            
            # Create data manager with the correct directory
            data_mgr = DataSharingManager(data_dir=results_dir)
            
            # Export daily candles to pkl (with historical merge)
            self.logger.info("Starting daily pkl export with historical merge...")
            success_daily, daily_path = data_mgr.export_daily_candles_to_pkl(self._candle_store, merge_with_historical=True)
            
            if success_daily and daily_path:
                file_size = os.path.getsize(daily_path) / (1024 * 1024)
                self.logger.info(f"Exported daily candles to: {daily_path} ({file_size:.2f} MB)")
                
                # USE ATOMIC WRITE for the date-suffixed copy
                _, file_name = Archiver.afterMarketStockDataExists()
                if file_name is not None and len(file_name) > 0:
                    today_suffix = file_name.replace('.pkl','').replace('stock_data_','')
                else:
                    today_suffix = datetime.now().strftime('%d%m%Y')
                
                dest_daily = os.path.join(results_dir, f"stock_data_{today_suffix}.pkl")
                if daily_path != dest_daily:
                    # Read the file and write atomically
                    with open(daily_path, 'rb') as src:
                        data = pickle.load(src)
                    self._atomic_pickle_dump(data, dest_daily)
                    self.logger.info(f"Created date-suffixed copy using atomic write: stock_data_{today_suffix}.pkl")
            else:
                self.logger.warning("⚠️ Daily pkl export failed or no data")
            
            # Export intraday candles to pkl
            self.logger.info("Starting intraday pkl export...")
            success_intraday, intraday_path, latest_date = data_mgr.export_intraday_candles_to_pkl(self._candle_store)
            if success_intraday and intraday_path:
                file_size = os.path.getsize(intraday_path) / (1024 * 1024)
                self.logger.info(f"Exported intraday candles for date {latest_date} to: {intraday_path} ({file_size:.2f} MB)")
                
                # Also create date-suffixed copy
                dest_intraday = os.path.join(results_dir, f"intraday_stock_data_{today_suffix}.pkl")
                if intraday_path != dest_intraday:
                    shutil.copy(intraday_path, dest_intraday)
                    self.logger.info(f"Created date-suffixed copy: intraday_stock_data_{today_suffix}.pkl")
            else:
                self.logger.warning("⚠️ Intraday pkl export from candle store failed, trying ticks.json fallback...")
                
                # Fallback: convert ticks.json directly to intraday pkl
                ticks_json_path = os.path.join(results_dir, "ticks.json")
                if not os.path.exists(ticks_json_path):
                    ticks_json_path = self.json_output_path
                
                if os.path.exists(ticks_json_path):
                    self.logger.info(f"Converting ticks.json from {ticks_json_path} to intraday pkl...")
                    success_ticks, ticks_pkl_path = data_mgr.convert_ticks_json_to_pkl(ticks_json_path)
                    if success_ticks:
                        self.logger.info(f"Successfully converted ticks.json to pkl: {ticks_pkl_path}")
                    else:
                        self.logger.warning("⚠️ Failed to convert ticks.json to pkl")
                else:
                    self.logger.warning(f"⚠️ No ticks.json found at {ticks_json_path}")
                
        except Exception as e:
            self.logger.error(f"🛑 🛑 🛑 🛑 Error exporting pkl files: {e}")

        # Signal shutdown to all components
        self._shutdown_event.set()

        # Stop JSON writer
        if self.json_writer:
            self.json_writer.stop()

        # Signal database thread to exit
        try:
            self._db_queue.put(None, timeout=2.0)
        except Exception:
            pass  # Queue might be full

        # Wait for threads with reasonable timeouts
        thread_timeout = THREAD_JOIN_TIMEOUT

        if self._processing_thread and self._processing_thread.is_alive():
            self._processing_thread.join(timeout=thread_timeout)
            if self._processing_thread.is_alive():
                self.logger.warning("⚠️ Processing thread did not terminate gracefully")

        # if self._db_thread and self._db_thread.is_alive():
        #     self._db_thread.join(timeout=thread_timeout)
        #     if self._db_thread.is_alive():
        #         self.logger.warning("⚠️ Database thread did not terminate gracefully")

        # Now shut down the executor (no new tasks will be submitted)
        self.executor.shutdown(wait=False)
        if self._client_watchdog_thread and self._client_watchdog_thread.is_alive():
            self._client_watchdog_thread.join(timeout=thread_timeout)
            if self._client_watchdog_thread.is_alive():
                self.logger.warning("⚠️ Client Watchdog thread did not terminate gracefully")

        self.logger.info("Shutdown complete")

    def __del__(self):
        """
        Ensure cleanup on object destruction.

        Serves as safety net for resource cleanup if stop() wasn't called.
        """
        if not self._shutdown_event.is_set():
            self.logger.debug("Auto-cleanup in destructor")
            self.stop()

    # DEBUG CRASH TEST REGION BELOW THIS LINE - USE WITH CAUTION - ONLY FOR TESTING RECOVERY AND RESILIENCE OF THE ORCHESTRATOR
    def _start_command_listener(self):
        """Listen for special commands from the orchestrator (used for failure simulation)."""
        def listen():
            while not self._shutdown_event.is_set():
                try:
                    if self._stop_queue and not self._stop_queue.empty():
                        cmd = self._stop_queue.get(timeout=0.5)
                        if cmd == "FILL_QUEUE":
                            self._simulate_queue_fill()
                        elif cmd == "DEADLOCK_CONSUMER":
                            self._simulate_deadlock()
                        elif cmd == "CRASH_CONSUMER":
                            self._simulate_crash()
                except Exception:
                    pass
                time.sleep(0.1)
        threading.Thread(target=listen, daemon=True).start()

    def _simulate_queue_fill(self):
        """Fill the _watcher_queue with dummy ticks to cause backpressure."""
        from pkbrokers.kite.ticks import Tick
        dummy_tick = Tick(
            instrument_token=999999,
            last_price=100,
            exchange_timestamp=time.time(),
            # other fields can be default
        )
        for _ in range(50000):  # 50k dummy ticks
            self._watcher_queue.put(dummy_tick)
        self.logger.warning("Simulated queue fill: 50k dummy ticks inserted")

    def _simulate_deadlock(self):
        """Acquire a lock and never release, causing the consumer thread to hang."""
        self._deadlock_lock = threading.Lock()
        self._deadlock_lock.acquire()
        self.logger.warning("Simulated deadlock – consumer will hang")
        # The deadlock will affect the next time the lock is needed; we can put a blocking get on a queue as well.

    def _simulate_crash(self):
        """Raise an unhandled exception inside the consumer thread."""
        self.logger.warning("Simulated crash – raising exception")
        raise RuntimeError("Simulated crash for testing recovery")