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
import time
import logging
import os
import sys
from typing import Optional

from pkbrokers.bot.consumer import PKTickBotConsumer
from pkbrokers.bot.tickbot import PKTickBot

class PKTickOrchestrator:
    """Orchestrates PKTickBot and kite_ticks in separate processes"""
    
    def __init__(self, bot_token: str, ticks_file_path: str, chat_id: Optional[str] = None):
        self.bot_token = bot_token
        self.ticks_file_path = ticks_file_path
        self.chat_id = chat_id
        self.bot_process = None
        self.kite_process = None
        self.logger = logging.getLogger(__name__)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def run_kite_ticks(self):
        """Run kite_ticks in a separate process"""
        from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher
        
        try:
            self.logger.info("Starting kite_ticks process...")
            watcher = KiteTokenWatcher()
            watcher.watch()
        except KeyboardInterrupt:
            self.logger.info("kite_ticks process interrupted")
        except Exception as e:
            self.logger.error(f"kite_ticks error: {e}")
    
    def run_telegram_bot(self):
        """Run Telegram bot in a separate process"""
        try:
            self.logger.info("Starting PKTickBot process...")
            bot = PKTickBot(self.bot_token, self.ticks_file_path, self.chat_id)
            bot.run()
        except Exception as e:
            self.logger.error(f"Telegram bot error: {e}")
    
    def start(self):
        """Start both processes"""
        self.logger.info("Starting PKTick Orchestrator...")
        
        # Start kite_ticks process
        self.kite_process = multiprocessing.Process(
            target=self.run_kite_ticks,
            name="KiteTicksProcess"
        )
        self.kite_process.daemon = True
        self.kite_process.start()
        
        # Wait a bit for data to start flowing
        time.sleep(5)
        
        # Start Telegram bot process
        self.bot_process = multiprocessing.Process(
            target=self.run_telegram_bot,
            name="TelegramBotProcess"
        )
        self.bot_process.daemon = True
        self.bot_process.start()
        
        self.logger.info("Both processes started successfully")
    
    def stop(self):
        """Stop both processes gracefully"""
        self.logger.info("Stopping processes...")
        
        if self.bot_process and self.bot_process.is_alive():
            self.bot_process.terminate()
            self.bot_process.join(timeout=5)
        
        if self.kite_process and self.kite_process.is_alive():
            self.kite_process.terminate()
            self.kite_process.join(timeout=5)
        
        self.logger.info("All processes stopped")
    
    def get_consumer(self) -> PKTickBotConsumer:
        """Get a consumer instance to interact with the bot"""
        if not self.chat_id:
            raise ValueError("chat_id is required for consumer functionality")
        return PKTickBotConsumer(self.bot_token, self.chat_id)
    
    def run(self):
        """Main run method with graceful shutdown handling"""
        try:
            self.start()
            
            # Keep main process alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        finally:
            self.stop()


# # Programmatic usage with zip handling
# consumer = PKTickBotConsumer('your_bot_token', 'your_chat_id')
# success, json_path = consumer.get_ticks(output_dir="./downloads")

# if success:
#     print(f"✅ Downloaded and extracted ticks.json to: {json_path}")
#     # Now you can use the JSON file
#     with open(json_path, 'r') as f:
#         data = json.load(f)
#     print(f"Found {len(data)} instruments")
# else:
#     print("❌ Failed to get ticks file")
