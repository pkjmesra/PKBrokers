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

# Use context manager for critical updates
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional


@contextmanager
def env_update_context(env_path):
    updater = EnvUpdater(env_path)
    backup = updater._read_current_content()
    try:
        yield updater
    except Exception:
        # Restore on failure
        updater._write_content(backup)
        raise


class EnvUpdater:
    def __init__(self, env_path: str = ".env.dev"):
        self.dev_path = Path(env_path)
        self._ensure_env_exists()

    def _ensure_env_exists(self):
        """Create .env.dev file if it doesn't exist"""
        if not self.dev_path.exists():
            self.dev_path.touch(mode=0o600)  # Secure permissions
            print(f"Created new .env.dev file at {self.dev_path}")

    def update_values(self, updates: Dict[str, str], quote_strings: bool = True):
        """
        Update or add multiple environment variables

        Args:
            updates: Dictionary of {key: value} pairs
            quote_strings: Whether to add quotes around string values
        """
        current_content = self._read_current_content()
        new_content = []

        # Process existing lines
        updated_keys = set()
        for line in current_content.splitlines():
            if not line.strip() or line.startswith("#"):
                new_content.append(line)
                continue

            key, _ = self._parse_line(line)
            if key in updates:
                new_value = self._format_value(updates[key], quote_strings)
                new_content.append(f"{key}={new_value}")
                updated_keys.add(key)
            else:
                new_content.append(line)

        # Add new keys
        for key, value in updates.items():
            if key not in updated_keys:
                new_value = self._format_value(value, quote_strings)
                new_content.append(f"{key}={new_value}")

        # Write back to file
        self._write_content("\n".join(new_content))

    def _parse_line(self, line: str) -> Optional[tuple]:
        """Extract key/value from .env.dev line"""
        line = line.strip()
        if "=" in line:
            key, value = line.split("=", 1)
            return key.strip(), value.strip().strip("\"'")
        return None

    def _format_value(self, value: str, quote: bool) -> str:
        """Format value according to .env.dev standards"""
        if quote and any(c in value for c in (" ", "#", "'", '"', "$")):
            return f'"{value}"'
        return value

    def _read_current_content(self) -> str:
        """Read existing content with proper encoding"""
        try:
            with open(self.dev_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def _write_content(self, content: str):
        """Atomic write operation"""
        temp_path = self.dev_path.with_suffix(".tmp")
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(content)
            temp_path.replace(self.dev_path)  # Atomic replace
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def reload_env(self):
        """Reload environment variables"""
        from dotenv import load_dotenv
        from PKDevTools.classes.Environment import PKEnvironment

        load_dotenv(self.dev_path, override=True)
        PKEnvironment()._load_secrets()
