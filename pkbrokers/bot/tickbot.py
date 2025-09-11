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

import html
import json
import logging
import os
import signal
import sys

try:
    import thread
except ImportError:
    import _thread as thread

import traceback
from datetime import datetime
from typing import Optional

from PKDevTools.classes.Environment import PKEnvironment
from telegram import Update
from telegram.ext import CallbackContext, CommandHandler, Updater

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
        self.ticks_db_path = ticks_file_path.replace(".json", ".db")
        self.chat_id = chat_id or PKEnvironment().CHAT_ID
        self.chat_id = (
            f"-{self.chat_id}"
            if not str(self.chat_id).startswith("-")
            else self.chat_id
        )
        self.updater = None
        self.logger = logging.getLogger(__name__)
        self.conflict_detected = False
        self.parent = None

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
            "Use /token to get the most recent token\n"
            "Use /refresh_token to generate and receive a new token\n"
            "Use /db to get the most recent db file\n"
            "Use /test_ticks to get a test ticks.json file\n"
            "Use /help for more information"
            "Use /start to start the bot\n"
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
            "/token - Sends the most recent saved token from environment\n"
            "/refresh_token - Generates, saves and sends the token\n"
            "/db - Get the most recent local SQLite DB file\n"
            "/test_ticks - Starts ticks for 3 minutes\n"
            "/help - Show this help message\n\n"
            "📦 Files are automatically compressed to reduce size. "
            "If the file is too large, it will be split into multiple parts."
        )

    def send_refreshed_token(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send refreshed token"""
        from PKDevTools.classes.Environment import PKEnvironment

        from pkbrokers.kite.examples.pkkite import kite_auth

        try:
            kite_auth()
            update.message.reply_text(PKEnvironment().KTOKEN)
        except Exception as e:
            update.message.reply_text(f"Could not generate/refresh token:{e}")

    def send_token(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send token"""
        update.message.reply_text(PKEnvironment().KTOKEN)

    def test_ticks(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        if self.parent and hasattr(self.parent,"bot_callback"):
            self.parent.bot_callback()

        def kite_trigger():
            from pkbrokers.kite.examples.pkkite import kite_ticks
            kite_ticks(test_mode=True)
        import threading
        kite_thread = threading.Thread(
            target=kite_trigger, daemon=True, name="kill_watcher"
        )
        kite_thread.start()
        
        if update is not None:
            update.message.reply_text(
                "Kite Tick testing kicked off! Try sending /ticks in sometime."
            )

    def send_zipped(self, file_name, file_path, update):
        try:
            if not os.path.exists(file_path):
                update.message.reply_text(
                    f"❌ {file_name} file not found yet. Please wait for data to be collected."
                )
                return

            file_size = os.path.getsize(file_path)
            if file_size == 0:
                update.message.reply_text(
                    f"⏳ {file_name} file is empty. Data collection might be in progress."
                )
                return

            # Create zip file
            from PKDevTools.classes import Fileinfo

            zip_path, zip_size = Fileinfo.create_zip_file(file_path)

            try:
                if zip_size <= self.MAX_FILE_SIZE and zip_size > 0:
                    # Send single file
                    with open(zip_path, "rb") as f:
                        update.message.reply_document(
                            document=f,
                            filename=f"{file_name}.zip",
                            caption=f"📈 Latest market data (compressed)\nOriginal: {file_size:,} bytes → Zipped: {zip_size:,} bytes",
                        )
                    self.logger.info(f"Sent zipped {file_name} file to user")

                elif zip_size > 0:
                    # File too large, need to split
                    update.message.reply_text(
                        f"📦 File {file_name} is too large ({zip_size:,} bytes). Splitting into parts..."
                    )

                    part_paths = Fileinfo.split_large_file(zip_path, self.MAX_FILE_SIZE)

                    for i, part_path in enumerate(part_paths, 1):
                        with open(part_path, "rb") as f:
                            update.message.reply_document(
                                document=f,
                                filename=f"{file_name}.part{i}.zip",
                                caption=f"For file {file_name}, Part {i} of {len(part_paths)}",
                            )
                        self.logger.info(
                            f"For file {file_name}, Sent part {i} of {len(part_paths)}"
                        )

                    update.message.reply_text(
                        f"✅ All parts of {file_name} sent! To reconstruct:\n"
                        + "1. Download all parts\n"
                        + f"2. Run: `cat {file_name}.part*.zip > {file_name}.zip`\n"
                        + f"3. Unzip: `unzip {file_name}.zip`"
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
            self.logger.error(f"Error sending zipped ticks file ({file_name}): {e}")
            update.message.reply_text(
                f"❌ Error preparing or sending file ({file_name}). Please try again later."
            )

    def send_zipped_ticks(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send zipped ticks.json file to user with size handling"""
        self.send_zipped("ticks.json", self.ticks_file_path, update)

    def send_zipped_db(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Send zipped ticks.db file to user with size handling"""
        self.send_zipped("ticks.db", self.ticks_db_path, update)

    def find_part_files(self, base_path: str) -> list:
        """Find any existing part files for a given base path"""
        import glob

        return glob.glob(f"{base_path}.part*")

    def get_top_ticks_formatted(self, limit=20):
        try:
            with open(self.ticks_file_path, "r") as f:
                data = json.load(f)
        except BaseException:
            return None

        instruments = list(data.values())
        top_limit = sorted(
            instruments, key=lambda x: x.get("tick_count", 0), reverse=True
        )[: limit + 2]
        output = None
        if len(top_limit) > 0:
            output = "Symbol         |Tick |Price\n"
            output += "---------------|-----|-------\n"
            NIFTY_50 = 256265
            BSE_SENSEX = 265
            for i, instrument in enumerate(top_limit, 1):
                instrument_token = instrument.get("instrument_token", 0)
                if instrument_token in [NIFTY_50, BSE_SENSEX]:
                    continue
                symbol = instrument.get("trading_symbol", "N/A")
                tick_count = instrument.get("tick_count", 0)
                price = instrument.get("ohlcv", {}).get("close", 0)

                output += f"{symbol:15}|{tick_count:4} | {price:6.1f}\n"

        return f"<pre>{html.escape(output)}</pre>"

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
        update.message.reply_text(message, parse_mode="HTML")

    def _update_stats(
        self, file_name: str = None, file_path: str = None, status_msg: str = None
    ):
        if os.path.exists(file_path):
            from PKDevTools.classes import Fileinfo

            f_info = Fileinfo.get_file_info(file_path)
            file_size = f_info.bytes
            status_msg += f"📁 {file_name}: {f_info.human_readable}\n"
            status_msg += (
                f"📁 Modified {f_info.seconds_ago} sec ago: {f_info.modified_ist}\n"
            )

            # Check zip size
            try:
                zip_path, zip_size = Fileinfo.create_zip_file(file_path)
                if zip_size > 0:
                    status_msg += f"📦 Compressed: {zip_size:,} bytes\n"
                    os.unlink(zip_path)  # Clean up temp zip

                    if zip_size > self.MAX_FILE_SIZE:
                        parts_needed = (
                            zip_size + self.MAX_FILE_SIZE - 1
                        ) // self.MAX_FILE_SIZE
                        status_msg += f"⚠️  Will be split into {parts_needed} parts\n"
                else:
                    status_msg += "📦 Compression Error\n"
            except Exception as e:
                status_msg += f"📦 Compression: Error ({e})\n"

            if file_size > 0:
                if file_name.endswith(".json"):
                    try:
                        with open(file_path, "r") as f:
                            data = json.load(f)
                        status_msg += f"📊 Instruments: {len(data):,}\n"
                    except BaseException:
                        status_msg += "📊 Instruments: File format error\n"
                elif file_name.endswith(".db"):
                    db_info = Fileinfo.get_sqlite_db_info(file_path)
                    if db_info:
                        status_msg += (
                            f"🛢️ Database file size: {db_info.file_size_human}\n"
                        )
                        status_msg += f"🛢️ Number of tables: {len(db_info.tables)}\n"
                        status_msg += f"🛢️ Tables: {', '.join(db_info.tables)}\n"
                        status_msg += f"🛢️ Total rows: {db_info.total_rows}\n"

                        for table, row_count in db_info.table_stats.items():
                            status_msg += f"🧱 {table}: {row_count} rows\n"
            else:
                status_msg += f"📊 {file_name}: File empty\n"
        else:
            status_msg += f"❌ {file_name}: Not found\n"
        return status_msg

    def status(self, update: Update, context: CallbackContext) -> None:
        if self._shouldAvoidResponse(update):
            if update is not None:
                update.message.reply_text(APOLOGY_TEXT)
            return
        """Check bot and data status"""
        try:
            status_msg = "✅ PKTickBot is online\n"
            status_msg = self._update_stats(
                "ticks.json", self.ticks_file_path, status_msg
            )
            status_msg = self._update_stats("ticks.db", self.ticks_db_path, status_msg)
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

        if start_time is not None:
            timeSinceStarted = datetime.now() - start_time
        else:
            timeSinceStarted = datetime.now()

        # Check for conflict error
        if "telegram.error.Conflict" in tb_string or "409" in tb_string:
            global conflict_detected
            conflict_detected = True
            self.conflict_detected = True
            logger.error(
                "Conflict detected: Another instance is running. Longer running instance should shut down gracefully."
            )

            if (
                timeSinceStarted.total_seconds() >= MINUTES_2_IN_SECONDS
            ):  # shutdown only if we have been running for over 2 minutes.
                warn_msg = f"❌ This instance is stopping due to conflict after running for {timeSinceStarted.total_seconds()/60} minutes."
                logger.warn(warn_msg)
                context.bot.send_message(
                    chat_id=int(f"-{Channel_Id}"), text=warn_msg, parse_mode="HTML"
                )
                try:
                    # Signal the main process to shutdown
                    os.kill(os.getpid(), signal.SIGINT)
                    try:
                        thread.interrupt_main()  # causes ctrl + c
                    except RuntimeError:
                        pass
                    except SystemExit:
                        thread.interrupt_main()
                except Exception as e:
                    logger.error(f"Error sending shutdown signal: {e}")
                    sys.exit(1)
            else:
                info_msg = (
                    "✅ Other instance is likely running! This instance will continue."
                )
                logger.warn(info_msg)
                context.bot.send_message(
                    chat_id=int(f"-{Channel_Id}"), text=info_msg, parse_mode="HTML"
                )

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
            if (
                "telegram.error.Conflict" not in tb_string
                and "409" not in tb_string
                and Channel_Id is not None
                and len(str(Channel_Id)) > 0
            ):
                context.bot.send_message(
                    chat_id=int(f"-{Channel_Id}"), text=message, parse_mode="HTML"
                )
        except Exception:
            try:
                if (
                    "telegram.error.Conflict" not in tb_string
                    and "409" not in tb_string
                    and Channel_Id is not None
                    and len(str(Channel_Id)) > 0
                ):
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
            dispatcher.add_handler(CommandHandler("db", self.send_zipped_db))
            dispatcher.add_handler(CommandHandler("test_ticks", self.test_ticks))
            dispatcher.add_handler(CommandHandler("status", self.status))
            dispatcher.add_handler(CommandHandler("top", self.top_ticks))
            dispatcher.add_handler(CommandHandler("token", self.send_token))
            dispatcher.add_handler(
                CommandHandler("refresh_token", self.send_refreshed_token)
            )

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

        if OWNER_USER in sentFrom or abs(int(chat_idADMIN)) in sentFrom:
            return False
            # We want to avoid sending any help message back to channel
            # or group in response to our own messages
        return True

    def run(self, parent=None):
        """Run the bot - no asyncio needed for v13.4"""
        if parent is not None:
            self.parent = parent
        self.run_bot()
