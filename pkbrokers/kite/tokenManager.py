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
import time
import fcntl
import pickle
import threading
from PKDevTools.classes import Archiver
from PKDevTools.classes.log import default_logger
from pkbrokers.kite.examples.externals import kite_auth

class TokenManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.logger = default_logger()
        self.cache_file = os.path.join(Archiver.get_user_data_dir(), "kite_token.cache")
        self.lock_file = self.cache_file + ".lock"
        self._initialized = True

    def _acquire_file_lock(self, timeout=30):
        """Acquire exclusive lock on the lock file."""
        lock_fd = open(self.lock_file, 'w')
        start = time.time()
        while True:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return lock_fd
            except BlockingIOError:
                if time.time() - start > timeout:
                    raise TimeoutError("Could not acquire token lock")
                time.sleep(0.5)

    def _release_file_lock(self, lock_fd):
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()

    def _read_cache(self):
        """Read token from cache file. Returns None if not exists or corrupted."""
        if not os.path.exists(self.cache_file):
            return None
        try:
            with open(self.cache_file, 'rb') as f:
                data = pickle.load(f)
            # Validate structure
            if isinstance(data, dict) and 'token' in data and 'timestamp' in data:
                return data
        except Exception:
            pass
        return None

    def _write_cache(self, token):
        data = {
            'token': token,
            'timestamp': time.time()
        }
        with open(self.cache_file, 'wb') as f:
            pickle.dump(data, f)

    def get_valid_token(self, force_refresh=False):
        """
        Returns a valid token. If the cached token is missing or force_refresh is True,
        acquires a lock and refreshes the token from the bot.
        """
        # First, try to read cached token (without lock)
        cache = self._read_cache()
        if not force_refresh and cache and cache.get('token'):
            self.logger.debug("Using cached token")
            return cache['token']

        # Need to refresh – acquire lock
        lock_fd = self._acquire_file_lock()
        try:
            # Double-check after acquiring lock (another process might have refreshed already)
            cache = self._read_cache()
            if not force_refresh and cache and cache.get('token'):
                self.logger.debug("Token refreshed by another process")
                return cache['token']

            # No valid token – request from bot
            self.logger.info("Requesting current token from bot...")
            from pkbrokers.bot.orchestrator import orchestrate_consumer
            token = orchestrate_consumer(command="/token")
            token = str(token).strip() if token else None

            # If token is invalid (None or too short), ask for a fresh one
            if not token or len(token) < 90:
                self.logger.warning("Current token invalid or missing, requesting /refresh_token...")
                token = orchestrate_consumer(command="/refresh_token")
                token = str(token).strip() if token else None

            if token and len(token) >= 90:
                self._write_cache(token)
                self.logger.info("Token refreshed and cached")
                self._save_update_environment(access_token=token)
                return token
            else:
                self.logger.error("🛑 🛑 🛑 🛑 Failed to obtain a valid token from bot")
                token = kite_auth()  # Fallback to direct auth if bot fails
                if token and len(token) >= 90:
                    self._write_cache(token)
                    self.logger.info("Token obtained via direct auth and cached")
                    return token
                return None
        finally:
            self._release_file_lock(lock_fd)

    def invalidate_token(self):
        """Force a fresh token on next call (e.g., after repeated 403s)."""
        self.logger.warning("Invalidating cached token")
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)

    def _save_update_environment(self, access_token: str = None):
        try:

            from PKDevTools.classes.Environment import PKEnvironment
            from PKDevTools.classes.log import default_logger

            from pkbrokers.envupdater import env_update_context

            os.environ["KTOKEN"] = access_token if access_token else PKEnvironment().KTOKEN
            default_logger().debug(f"Token received: {access_token}")
            with env_update_context(os.path.join(os.getcwd(), ".env.dev")) as updater:
                updater.update_values({"KTOKEN": access_token})
                updater.reload_env()
                default_logger().debug(
                    f"Token updated in os.environment: {PKEnvironment().KTOKEN}"
                )
        except Exception as e:
            default_logger().error(f"🛑 🛑 🛑 🛑 Error while updating token in the PKEnvironment: {e}")
