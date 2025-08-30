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
import sys
import time
import multiprocessing
from typing import Optional

# macOS fork safety
if sys.platform.startswith("darwin"):
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
    os.environ["NO_FORK_SAFETY"] = "YES"

if __name__ == "__main__":
    multiprocessing.freeze_support()

# Set spawn context globally
multiprocessing.set_start_method("spawn", force=True)

class PKTickOrchestrator:
    """Orchestrates PKTickBot and kite_ticks in separate processes"""

    def __init__(
        self, bot_token: Optional[str] = None, bridge_bot_token: Optional[str] = None, ticks_file_path: Optional[str] = None, chat_id: Optional[str] = None
    ):
        # Store only primitive data types that can be pickled
        self.bot_token = bot_token
        self.bridge_bot_token = bridge_bot_token
        self.ticks_file_path = ticks_file_path
        self.chat_id = chat_id
        self.bot_process = None
        self.kite_process = None
        self.mp_context = multiprocessing.get_context("spawn")
        
        # Don't initialize logger or other complex objects here
        # They will be initialized in each process separately

    def __getstate__(self):
        """Control what gets pickled - only include primitive data"""
        state = self.__dict__.copy()
        # Remove unpickleable objects
        for key in ['bot_process', 'kite_process', 'mp_context', 'logger']:
            state.pop(key, None)
        return state

    def __setstate__(self, state):
        """Restore state after unpickling"""
        self.__dict__.update(state)
        # Reinitialize multiprocessing context
        self.mp_context = multiprocessing.get_context("spawn")
        self.bot_process = None
        self.kite_process = None

    def _get_logger(self):
        """Get logger instance - initialized separately in each process"""
        from PKDevTools.classes import log
        from pkbrokers.kite.examples.pkkite import setupLogger
        setupLogger()
        return log.default_logger()

    def _initialize_environment(self):
        """Initialize environment variables if not provided"""
        if not self.bot_token or not self.chat_id or not self.ticks_file_path:
            from PKDevTools.classes import Archiver
            from PKDevTools.classes.Environment import PKEnvironment
            
            env = PKEnvironment()
            self.bot_token = self.bot_token or env.TBTOKEN
            self.bridge_bot_token = self.bridge_bot_token or env.BBTOKEN
            self.chat_id = self.chat_id or env.CHAT_ID
            self.ticks_file_path = self.ticks_file_path or os.path.join(
                Archiver.get_user_data_dir(), "ticks.json"
            )

    def run_kite_ticks(self):
        """Run kite_ticks in a separate process"""
        try:
            # Initialize environment and logger in this process
            self._initialize_environment()
            logger = self._get_logger()
            
            from pkbrokers.kite.examples.pkkite import kite_auth, kite_ticks, setupLogger
            
            logger.info("Starting kite_ticks process...")
            kite_auth()
            kite_ticks()
        except KeyboardInterrupt:
            logger.info("kite_ticks process interrupted")
        except Exception as e:
            logger.error(f"kite_ticks error: {e}")

    def run_telegram_bot(self):
        """Run Telegram bot in a separate process"""
        try:
            # Initialize environment and logger in this process
            self._initialize_environment()
            logger = self._get_logger()
            
            from pkbrokers.bot.tickbot import PKTickBot
            
            logger.info("Starting PKTickBot process...")
            
            # Create and run the bot
            bot = PKTickBot(self.bot_token, self.ticks_file_path, self.chat_id)
            bot.run()
            
        except Exception as e:
            logger.error(f"Telegram bot error: {e}")

    def start(self):
        """Start both processes"""
        # Initialize logger in main process
        logger = self._get_logger()
        logger.info("Starting PKTick Orchestrator...")
        
        # Start kite_ticks process
        self.kite_process = self.mp_context.Process(
            target=self.run_kite_ticks, name="KiteTicksProcess"
        )
        self.kite_process.daemon = False
        self.kite_process.start()

        # Wait a bit for data to start flowing
        time.sleep(5)

        # Start Telegram bot process
        self.bot_process = self.mp_context.Process(
            target=self.run_telegram_bot, name="PKTickBotProcess"
        )
        self.bot_process.daemon = False
        self.bot_process.start()

        logger.info("Both processes started successfully")

    def stop(self):
        """Stop both processes gracefully"""
        logger = self._get_logger()
        logger.info("Stopping processes...")

        if self.bot_process and self.bot_process.is_alive():
            self.bot_process.terminate()
            self.bot_process.join(timeout=5)

        if self.kite_process and self.kite_process.is_alive():
            self.kite_process.terminate()
            self.kite_process.join(timeout=5)

        logger.info("All processes stopped")

    def get_consumer(self):
        """Get a consumer instance to interact with the bot"""
        self._initialize_environment()
        from pkbrokers.bot.consumer import PKTickBotConsumer
        if not self.chat_id:
            raise ValueError("chat_id is required for consumer functionality")
        return PKTickBotConsumer(self.bot_token,self.bridge_bot_token, self.chat_id)

    def run(self):
        """Main run method with graceful shutdown handling"""
        try:
            self.start()

            # Keep main process alive and monitor child processes
            logger = self._get_logger()
            while True:
                time.sleep(1)
                
                # Check if bot process died
                if self.bot_process and not self.bot_process.is_alive():
                    logger.warn("Bot process died, restarting...")
                    self.bot_process = self.mp_context.Process(
                        target=self.run_telegram_bot, name="PKTickBotProcess"
                    )
                    self.bot_process.daemon = False
                    self.bot_process.start()
                
                # Check if kite process died
                if self.kite_process and not self.kite_process.is_alive():
                    logger.warn("Kite ticks process died, restarting...")
                    self.kite_process = self.mp_context.Process(
                        target=self.run_kite_ticks, name="KiteTicksProcess"
                    )
                    self.kite_process.daemon = False
                    self.kite_process.start()

        except KeyboardInterrupt:
            logger = self._get_logger()
            logger.info("Keyboard interrupt received")
        finally:
            self.stop()


def orchestrate():
    # Initialize with None values, they will be set from environment when needed
    orchestrator = PKTickOrchestrator(None, None, None, None)
    orchestrator.run()

def orchestrate_consumer():
    from PKDevTools.classes import Archiver
    import json
    from pkbrokers.bot.consumer import try_get_ticks_from_bot
    # Programmatic usage with zip handling
    # orchestrator = PKTickOrchestrator(None, None, None, None)
    # consumer = orchestrator.get_consumer()
    # success, json_path = consumer.get_ticks(output_dir=os.path.join(Archiver.get_user_data_dir()))
    success = try_get_ticks_from_bot()
    json_path = os.path.join(Archiver.get_user_data_dir(), "extracted" ,"ticks.json")
    success = os.path.exists(json_path)
    if success:
        print(f"✅ Downloaded and extracted ticks.json to: {json_path}")
        # Now you can use the JSON file
        with open(json_path, 'r') as f:
            data = json.load(f)
        print(f"Found {len(data)} instruments")
    else:
        print("❌ Failed to get ticks file")
