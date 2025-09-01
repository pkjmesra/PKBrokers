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

import json
import html
import logging
import os
import tempfile
import zipfile
import pytz
import signal
import sys
try:
    import thread
except ImportError:
    import _thread as thread

import traceback

from typing import Optional, Tuple
from datetime import datetime

from telegram import Update
from telegram.ext import CommandHandler, CallbackContext, Updater, CallbackContext
from PKDevTools.classes import Archiver
from PKDevTools.classes.Environment import PKEnvironment

MINUTES_2_IN_SECONDS = 120
OWNER_USER = "Itsonlypk"
GROUP_CHAT_ID = 1001907892864
start_time = datetime.now()
APOLOGY_TEXT = "Apologies! The @pktickbot is NOT available for the time being! We are working with our host GitHub and other data source providers to sort out pending invoices and restore the services soon! Thanks for your patience and support! 🙏"

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Global variable to track conflict state
conflict_detected = False

class PKTickBot:
    """Telegram bot that sends zipped ticks.json file on command"""

    # Telegram file size limits (50MB for documents)
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

    def __init__(
        self, bot_token: str, ticks_file_path: str, chat_id: Optional[str] = None
    ):
        self.bot_token = bot_token
        self.ticks_file_path = ticks_file_path
        self.chat_id = chat_id or PKEnvironment().CHAT_ID
        self.chat_id = f"-{self.chat_id}" if not str(self.chat_id).startswith("-") else self.chat_id
        self.updater = None
        self.logger = logging.getLogger(__name__)
        self.conflict_detected = False

    def start(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send welcome message"""
        update.message.reply_text(
            "📊 PKTickBot is running!\n"
            "Use /ticks to get the latest market data JSON file (zipped)\n"
            "Use /status to check bot status\n"
            "Use /top to Get top 20 ticking symbols\n"
            "Use /help for more information"
        )

    def help_command(self, update: Update, context: CallbackContext) -> None:
        """Send help message"""
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        update.message.reply_text(
            "🤖 PKTickBot Commands:\n"
            "/start - Start the bot\n"
            "/ticks - Get zipped market data file\n"
            "/status - Check bot and data status\n"
            "/top - Get top 20 ticking symbols\n"
            "/help - Show this help message\n\n"
            "📦 Files are automatically compressed to reduce size. "
            "If the file is too large, it will be split into multiple parts."
        )

    def create_zip_file(self, json_path: str) -> Tuple[str, int]:
        """Create a zip file from JSON and return (zip_path, file_size)"""
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
            zip_path = tmp_zip.name

        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(json_path, os.path.basename(json_path))

            file_size = os.path.getsize(zip_path)
            return zip_path, file_size

        except Exception as e:
            self.logger.error(f"Error creating zip file: {e}")
            # Clean up on error
            if os.path.exists(zip_path):
                os.unlink(zip_path)
            raise

    def split_large_file(self, file_path: str, max_size: int) -> list:
        """Split large file into multiple parts and return list of part paths"""
        part_paths = []
        part_num = 1

        try:
            with open(file_path, "rb") as src_file:
                while True:
                    part_filename = f"{file_path}.part{part_num}"
                    with open(part_filename, "wb") as part_file:
                        data = src_file.read(max_size)
                        if not data:
                            break
                        part_file.write(data)

                    part_paths.append(part_filename)
                    part_num += 1

            return part_paths

        except BaseException:
            # Clean up any created parts on error
            for part_path in part_paths:
                if os.path.exists(part_path):
                    os.unlink(part_path)
            raise

    def send_refreshed_token(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send refreshed token"""
        from PKDevTools.classes.Environment import PKEnvironment
        from pkbrokers.kite.authenticator import KiteAuthenticator

        credentials = {
            "api_key": "kitefront",
            "username": PKEnvironment().KUSER,
            "password": PKEnvironment().KPWD,
            "totp": PKEnvironment().KTOTP,
        }
        authenticator = KiteAuthenticator(timeout=10)
        authenticator.get_enctoken(**credentials)
        update.message.reply_text(PKEnvironment().KTOKEN)

    def send_token(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send token"""
        update.message.reply_text(PKEnvironment().KTOKEN)

    def send_zipped_ticks(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send zipped ticks.json file to user with size handling"""
        try:
            if not os.path.exists(self.ticks_file_path):
                update.message.reply_text(
                    "❌ ticks.json file not found yet. Please wait for data to be collected."
                )
                return

            file_size = os.path.getsize(self.ticks_file_path)
            if file_size == 0:
                update.message.reply_text(
                    "⏳ ticks.json file is empty. Data collection might be in progress."
                )
                return

            # Create zip file
            zip_path, zip_size = self.create_zip_file(self.ticks_file_path)

            try:
                if zip_size <= self.MAX_FILE_SIZE:
                    # Send single file
                    with open(zip_path, "rb") as f:
                        update.message.reply_document(
                            document=f,
                            filename="market_ticks.zip",
                            caption=f"📈 Latest market data (compressed)\nOriginal: {file_size:,} bytes → Zipped: {zip_size:,} bytes",
                        )
                    self.logger.info("Sent zipped ticks file to user")

                else:
                    # File too large, need to split
                    update.message.reply_text(
                        f"📦 File is too large ({zip_size:,} bytes). Splitting into parts..."
                    )

                    part_paths = self.split_large_file(zip_path, self.MAX_FILE_SIZE)

                    for i, part_path in enumerate(part_paths, 1):
                        with open(part_path, "rb") as f:
                            update.message.reply_document(
                                document=f,
                                filename=f"market_ticks.part{i}.zip",
                                caption=f"Part {i} of {len(part_paths)}",
                            )
                        self.logger.info(f"Sent part {i} of {len(part_paths)}")

                    update.message.reply_text(
                        "✅ All parts sent! To reconstruct:\n"
                        + "1. Download all parts\n"
                        + "2. Run: `cat market_ticks.part*.zip > market_ticks.zip`\n"
                        + "3. Unzip: `unzip market_ticks.zip`"
                    )

            finally:
                # Clean up temporary files
                if os.path.exists(zip_path):
                    os.unlink(zip_path)
                # Clean up any part files if they exist
                for part_path in self.find_part_files(zip_path):
                    if os.path.exists(part_path):
                        os.unlink(part_path)

        except Exception as e:
            self.logger.error(f"Error sending zipped ticks file: {e}")
            update.message.reply_text(
                "❌ Error preparing or sending file. Please try again later."
            )

    def find_part_files(self, base_path: str) -> list:
        """Find any existing part files for a given base path"""
        import glob
        return glob.glob(f"{base_path}.part*")

    def get_top_ticks_formatted(self, limit=20):
        with open(self.ticks_file_path, 'r') as f:
            data = json.load(f)
        
        instruments = list(data.values())
        top_limit = sorted(instruments, key=lambda x: x.get('tick_count', 0), reverse=True)[:limit]
        output = None
        if len(top_limit) > 0:
            output = "Symbol         |Tick|Price\n"
            output += "---------------|----|-------\n"
            
            for i, instrument in enumerate(top_limit, 1):
                symbol = instrument.get('trading_symbol', 'N/A')
                tick_count = instrument.get('tick_count', 0)
                price = instrument.get('ohlcv', {}).get('close', 0)
                
                output += f"{symbol:15}|{tick_count:4} | {price:6.1f}\n"
        
        return output

    def top_ticks(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send top 20 instruments by tick count"""
        top_instruments = self.get_top_ticks_formatted(limit=20)
        if not top_instruments:
            update.message.reply_text("No data available or error reading ticks file.")
            return
        message = f"📊 Top 20 Instruments by Tick Count:\n\n{top_instruments}"
        update.message.reply_text(message)

    def status(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Check bot and data status"""
        try:
            status_msg = "✅ PKTickBot is online\n"

            if os.path.exists(self.ticks_file_path):
                file_size = os.path.getsize(self.ticks_file_path)
                file_mtime = Archiver.get_last_modified_datetime(self.ticks_file_path)
                file_mtime_str = file_mtime.strftime("%Y-%m-%d %H:%M:%S %Z")
                curr = datetime.now(pytz.timezone("Asia/Kolkata"))
                seconds_ago = (curr - file_mtime).seconds
                status_msg += f"📁 ticks.json: {file_size:,} bytes\n"
                status_msg += f"📁 Modified {seconds_ago} sec ago: {file_mtime_str}\n"

                # Check zip size
                try:
                    zip_path, zip_size = self.create_zip_file(self.ticks_file_path)
                    status_msg += f"📦 Compressed: {zip_size:,} bytes\n"
                    os.unlink(zip_path)  # Clean up temp zip

                    if zip_size > self.MAX_FILE_SIZE:
                        parts_needed = (zip_size + self.MAX_FILE_SIZE - 1) // self.MAX_FILE_SIZE
                        status_msg += f"⚠️  Will be split into {parts_needed} parts\n"

                except Exception as e:
                    status_msg += f"📦 Compression: Error ({e})\n"

                if file_size > 0:
                    try:
                        with open(self.ticks_file_path, "r") as f:
                            data = json.load(f)
                        status_msg += f"📊 Instruments: {len(data):,}\n"
                    except BaseException:
                        status_msg += "📊 Instruments: File format error\n"
                else:
                    status_msg += "📊 Instruments: File empty\n"
            else:
                status_msg += "❌ ticks.json: Not found\n"

            update.message.reply_text(status_msg)

        except Exception as e:
            self.logger.error(f"Error in status command: {e}")
            update.message.reply_text("❌ Error checking status")

    def error_handler(self, update: object, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update) and update is not None:
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        
        Channel_Id = PKEnvironment().CHAT_ID
        """Log the error and send a telegram message to notify the developer."""
        # Log the error before we do anything else, so we can see it even if something breaks.
        logger.error("Exception while handling an update:", exc_info=context.error)

        # traceback.format_exception returns the usual python message about an exception, but as a
        # list of strings rather than a single string, so we have to join them together.
        tb_list = traceback.format_exception(
            None, context.error, context.error.__traceback__
        )
        tb_string = "".join(tb_list)
        global start_time
        timeSinceStarted = datetime.now() - start_time
        
        # Check for conflict error
        if "telegram.error.Conflict" in tb_string or "409" in tb_string:
            global conflict_detected
            conflict_detected = True
            self.conflict_detected = True
            logger.error("Conflict detected: Another instance is running. Longer running instance should shut down gracefully.")
            
            if (
                timeSinceStarted.total_seconds() >= MINUTES_2_IN_SECONDS
            ):  # shutdown only if we have been running for over 2 minutes.
                logger.info(f"Stopping due to conflict after running for {timeSinceStarted.total_seconds()/60} minutes.")
                try:
                    # Signal the main process to shutdown
                    os.kill(os.getpid(), signal.SIGINT)
                    try:
                        thread.interrupt_main() # causes ctrl + c
                    except RuntimeError:
                        pass
                    except SystemExit:
                        thread.interrupt_main()
                except Exception as e:
                    logger.error(f"Error sending shutdown signal: {e}")
                    sys.exit(1)
            else:
                logger.info("Other instance running! This instance will continue.")
        
        # Build the message with some markup and additional information about what happened.
        update_str = update.to_dict() if isinstance(update, Update) else str(update)
        message = (
            f"An exception was raised while handling an update\n"
            f"<pre>update = {html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}"
            "</pre>\n\n"
            f"<pre>context.chat_data = {html.escape(str(context.chat_data))}</pre>\n\n"
            f"<pre>context.user_data = {html.escape(str(context.user_data))}</pre>\n\n"
            f"<pre>{html.escape(tb_string)}</pre>"
        )

        try:
            # Finally, send the message only if it's not a conflict error
            if "telegram.error.Conflict" not in tb_string and "409" not in tb_string and Channel_Id is not None and len(str(Channel_Id)) > 0:
                context.bot.send_message(
                    chat_id=int(f"-{Channel_Id}"), text=message, parse_mode="HTML"
                )
        except Exception:
            try:
                if "telegram.error.Conflict" not in tb_string and "409" not in tb_string and Channel_Id is not None and len(str(Channel_Id)) > 0:
                    context.bot.send_message(
                        chat_id=int(f"-{Channel_Id}"),
                        text=tb_string,
                        parse_mode="HTML",
                    )
            except Exception:
                logger.error(tb_string)

    def run_bot(self):
        """Run the telegram bot - synchronous version for v13.4"""
        try:
            self.updater = Updater(self.bot_token, use_context=True)
            dispatcher = self.updater.dispatcher

            # Add handlers
            dispatcher.add_handler(CommandHandler("start", self.start))
            dispatcher.add_handler(CommandHandler("ticks", self.send_zipped_ticks))
            dispatcher.add_handler(CommandHandler("status", self.status))
            dispatcher.add_handler(CommandHandler("top", self.top_ticks))
            dispatcher.add_handler(CommandHandler("token", self.send_token))
            dispatcher.add_handler(CommandHandler("refresh_token", self.send_refreshed_token))
            
            dispatcher.add_handler(CommandHandler("help", self.help_command))
            dispatcher.add_error_handler(self.error_handler)
            self.logger.info("Starting PKTickBot...")

            if self.chat_id:
                # Send startup message to specific chat
                try:
                    self.updater.bot.send_message(
                        chat_id=self.chat_id, text="🚀 PKTickBot started successfully!"
                    )
                except Exception as e:
                    self.logger.warn(f"Could not send startup message: {e}")

            # Start polling
            self.updater.start_polling()
            
            # Run the bot until interrupted
            self.updater.idle()

        except Exception as e:
            self.logger.error(f"Bot error: {e}")
            raise
        finally:
            if self.updater:
                self.updater.stop()
                self.logger.info("Bot stopped gracefully")
            # If conflict was detected, stop the updater
            if self.conflict_detected:
                os._exit(1)  # Use os._exit to bypass finally blocks

    def _shouldAvoidResponse(self, update):
        chat_idADMIN = PKEnvironment().chat_idADMIN
        sentFrom = []
        if update is None:
            return True
        if update.callback_query is not None:
            sentFrom.append(abs(update.callback_query.from_user.id))
        if update.message is not None and update.message.from_user is not None:
            sentFrom.append(abs(update.message.from_user.id))
            if update.message.from_user.username is not None:
                sentFrom.append(update.message.from_user.username)
        if update.channel_post is not None:
            if update.channel_post.chat is not None:
                sentFrom.append(abs(update.channel_post.chat.id))
                if update.channel_post.chat.username is not None:
                    sentFrom.append(update.channel_post.chat.username)
            if update.channel_post.sender_chat is not None:
                sentFrom.append(abs(update.channel_post.sender_chat.id))
                sentFrom.append(update.channel_post.sender_chat.username)
        if update.edited_channel_post is not None:
            sentFrom.append(abs(update.edited_channel_post.sender_chat.id))

        if (
            OWNER_USER in sentFrom
            or abs(int(chat_idADMIN)) in sentFrom
        ):
            return False
            # We want to avoid sending any help message back to channel
            # or group in response to our own messages
        return True

    def run(self):
        """Run the bot - no asyncio needed for v13.4"""
        self.run_bot()
