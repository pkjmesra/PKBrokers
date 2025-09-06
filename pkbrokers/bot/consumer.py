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

import asyncio
import logging
import os
import re
import time
import zipfile
from typing import List, Optional, Tuple

import requests
from PKDevTools.classes import Archiver
from PKDevTools.classes.Environment import PKEnvironment
from telethon import TelegramClient, events


class PKTickBotConsumer:
    """Programmatic client to interact with PKTickBot with zip handling"""

    def __init__(self, bot_token: str, bridge_bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.bridge_bot_token = bridge_bot_token
        self.chat_id = chat_id
        self.bridge_base_url = f"https://api.telegram.org/bot{bridge_bot_token}"
        self.logger = logging.getLogger(__name__)

    def get_updates(
        self, timeout: int = 30, offset: Optional[int] = None
    ) -> List[dict]:
        """Get recent updates from the bot"""
        try:
            url = f"{self.bridge_base_url}/getUpdates"
            params = {"timeout": timeout, "offset": offset}

            response = requests.get(url, params=params, timeout=timeout + 5)
            response.raise_for_status()

            return response.json().get("result", [])

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error getting updates: {e}")
            return []

    def send_command(self, command: str = "/ticks") -> bool:
        """Send a command to the bot"""
        try:
            url = f"{self.bridge_base_url}/sendMessage"
            payload = {
                "chat_id": 8423093422,  # The main bot's username
                "text": command,
            }

            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()

            self.logger.info(f"Successfully sent command: {command}")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending command: {e}")
            return False

    def download_file(self, file_id: str, file_path: str) -> bool:
        """Download a file from Telegram"""
        try:
            # Get file path
            url = f"{self.bridge_base_url}/getFile"
            payload = {"file_id": file_id}
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()

            file_path_info = response.json()["result"]["file_path"]

            # Download file
            download_url = f"https://api.telegram.org/file/bot{self.bridge_bot_token}/{file_path_info}"
            response = requests.get(download_url, stream=True, timeout=60)
            response.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error downloading file: {e}")
            return False

    def extract_zip(self, zip_path: str, extract_path: str) -> bool:
        """Extract zip file and return success status"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_path)
            return True
        except Exception as e:
            self.logger.error(f"Error extracting zip: {e}")
            return False

    def reassemble_parts(self, part_paths: List[str], output_path: str) -> bool:
        """Reassemble split zip parts into single file"""
        try:
            with open(output_path, "wb") as output_file:
                for part_path in sorted(part_paths):
                    with open(part_path, "rb") as part_file:
                        output_file.write(part_file.read())
            return True
        except Exception as e:
            self.logger.error(f"Error reassembling parts: {e}")
            return False

    def get_ticks(
        self, output_dir: str = ".", timeout: int = 120
    ) -> Tuple[bool, Optional[str]]:
        """Request ticks.json file and download/process it"""
        try:
            # Send command
            if not self.send_command("/ticks"):
                return False, "Failed to send command"

            self.logger.info("Waiting for bot response...")

            # Wait for file messages
            start_time = time.time()
            last_update_id = None
            downloaded_files = []
            zip_parts = {}

            while time.time() - start_time < timeout:
                updates = self.get_updates(10, last_update_id)

                for update in updates:
                    last_update_id = update["update_id"] + 1

                    if "message" in update and "document" in update["message"]:
                        doc = update["message"]["document"]
                        file_name = doc.get("file_name", "")
                        file_id = doc["file_id"]

                        # Determine file type
                        if file_name.endswith(".zip"):
                            # Single zip file
                            output_path = os.path.join(output_dir, file_name)
                            if self.download_file(file_id, output_path):
                                downloaded_files.append(output_path)
                                self.logger.info(f"Downloaded: {file_name}")

                        elif re.match(r".*\.part\d+\.zip$", file_name):
                            # Part of split zip
                            part_match = re.search(r"\.part(\d+)\.zip$", file_name)
                            if part_match:
                                part_num = int(part_match.group(1))
                                output_path = os.path.join(output_dir, file_name)
                                if self.download_file(file_id, output_path):
                                    zip_parts[part_num] = output_path
                                    self.logger.info(
                                        f"Downloaded part {part_num}: {file_name}"
                                    )

                # Check if we have all parts or the complete file
                if downloaded_files:  # Single file case
                    break

                if zip_parts:  # Multi-part case
                    expected_parts = max(zip_parts.keys()) if zip_parts else 0
                    if len(zip_parts) == expected_parts:
                        break

                time.sleep(2)

            # Process downloaded files
            if downloaded_files:
                # Single zip file case
                zip_path = downloaded_files[0]
                extract_dir = os.path.join(output_dir, "")
                os.makedirs(extract_dir, exist_ok=True)

                if self.extract_zip(zip_path, extract_dir):
                    json_path = os.path.join(extract_dir, "market_ticks.json")
                    if os.path.exists(json_path):
                        return True, json_path

            elif zip_parts:
                # Multi-part case - reassemble
                sorted_parts = [zip_parts[i] for i in sorted(zip_parts.keys())]
                assembled_zip = os.path.join(output_dir, "market_ticks_assembled.zip")

                if self.reassemble_parts(sorted_parts, assembled_zip):
                    extract_dir = os.path.join(output_dir, "")
                    os.makedirs(extract_dir, exist_ok=True)

                    if self.extract_zip(assembled_zip, extract_dir):
                        json_path = os.path.join(extract_dir, "market_ticks.json")
                        if os.path.exists(json_path):
                            # Clean up parts
                            for part_path in sorted_parts:
                                os.unlink(part_path)
                            os.unlink(assembled_zip)
                            return True, json_path

            return False, "No valid file received or extraction failed"

        except Exception as e:
            self.logger.error(f"Error in get_ticks: {e}")
            return False, str(e)

    def get_status(self) -> bool:
        """Request bot status"""
        return self.send_command("/status")


async def get_pktickbot_response_command(command: str = "/ticks"):
    """Enhanced version with better error handling and response parsing"""
    api_id = PKEnvironment().Tel_API_ID
    api_hash = PKEnvironment().Tel_API_Hash
    phone_number = PKEnvironment().Tel_Phone_Number

    async with TelegramClient("user_session", api_id, api_hash) as client:
        await client.start(phone=phone_number)

        response_queue = asyncio.Queue()
        response_received = asyncio.Event()

        # Send command to the bot
        bot_username = "@pktickbot"
        await client.send_message(bot_username, command)
        print(f"Command '{command}' sent to bot")

        # Handler for bot responses
        @client.on(events.NewMessage(from_users=bot_username))
        async def handler(event):
            try:
                response = {"type": None, "content": None, "raw_message": event.message}

                if event.message.document:
                    response["type"] = "file"
                    response["file_name"] = (
                        event.message.document.attributes[0].file_name
                        if event.message.document.attributes
                        else "unknown"
                    )
                    response["file_size"] = event.message.document.size

                elif event.message.text:
                    response["type"] = "text"
                    response["content"] = event.message.text

                elif event.message.photo:
                    response["type"] = "photo"

                # Put response in queue and signal receipt
                await response_queue.put(response)
                response_received.set()

            except Exception as e:
                error_response = {"type": "error", "content": str(e), "success": False}
                await response_queue.put(error_response)
                response_received.set()
            finally:
                client.remove_event_handler(handler)

        # Wait for response with timeout
        try:
            # Wait for the response received signal
            await asyncio.wait_for(response_received.wait(), timeout=60)

            # Get the response from queue
            response = await response_queue.get()

            # Process file downloads if needed
            if response["type"] in ["file", "photo"] and not response.get("content"):
                file_path = await response["raw_message"].download_media(
                    file=Archiver.get_user_data_dir()
                )
                response["content"] = file_path

                # Extract zip files
                if response["type"] == "file" and file_path.endswith(".zip"):
                    extract_dir = os.path.join(Archiver.get_user_data_dir(), "")
                    os.makedirs(extract_dir, exist_ok=True)
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        zip_ref.extractall(extract_dir)
                    response["extracted_path"] = extract_dir

            response["success"] = True
            return response

        except asyncio.TimeoutError:
            return {
                "type": "timeout",
                "content": "No response from bot within 60 seconds",
                "success": False,
            }


def try_get_command_response_from_bot(command: str = "/ticks"):
    # Run it
    return asyncio.run(get_pktickbot_response_command(command=command))


# # encode_session.py (run locally)
# import base64

# with open('user_session.session', 'rb') as f:
#     session_data = base64.b64encode(f.read()).decode('utf-8')

# print(f"::add-mask::{session_data}")
# print(f"SESSION_DATA={session_data}")

# SESSION_DATA: ${{ secrets.SESSION_DATA }}
# echo $SESSION_DATA | base64 -d > user_session.session
# chmod 600 user_session.session
