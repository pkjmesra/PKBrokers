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
from datetime import datetime
from threading import Lock
from typing import Dict, List, Union

import requests
from PKDevTools.classes.PKDateUtilities import PKDateUtilities


class KiteTickerHistory:
    """
    Fetches historical data from Zerodha's Kite Connect API with:
    - Proper cookie handling from access_token_response
    - Strict rate limiting (3 requests/second)
    - Batch processing with automatic retries

    Usage:
        authenticator = KiteAuthenticator()
        enctoken = authenticator.get_enctoken(...)

        history = KiteTickerHistory(
            enctoken=enctoken,
            user_id="YourUserId",
            auth_cookies=authenticator.access_token_response.headers.get('Set-Cookie', '')
        )
    """

    BASE_URL = "https://kite.zerodha.com/oms/instruments/historical"
    RATE_LIMIT = 3  # requests per second
    RATE_LIMIT_WINDOW = 1.0  # seconds

    def __init__(
        self,
        enctoken: str = None,
        user_id: str = None,
        access_token_response: requests.Response = None,
    ):
        """
        Initialize with authentication token and cookies

        Args:
            enctoken: Authentication token from KiteAuthenticator
            user_id: Zerodha user ID (e.g., 'YourUserId')
            access_token_response: Cookies/headers from access_token_response (along with Set-Cookie headers)
        """
        from dotenv import dotenv_values

        local_secrets = dotenv_values(".env.dev")

        if enctoken is None or len(enctoken) == 0:
            enctoken = (
                os.environ.get(
                    "KTOKEN", local_secrets.get("KTOKEN", "You need your Kite token")
                ),
            )
        if user_id is None or len(user_id) == 0:
            user_id = os.environ.get(
                "KUSER", local_secrets.get("KUSER", "You need your Kite user")
            )
        self.enctoken = enctoken
        self.user_id = user_id
        self.session = requests.Session()
        self.last_request_time = 0
        self.lock = Lock()  # For thread-safe rate limiting

        # Set all required headers and cookies
        self.session.headers.update(
            {
                "Authorization": f"enctoken {self.enctoken}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "X-Kite-Version": "3.0.0",
            }
        )

        # Copy all cookies from the auth response
        self.session.cookies.update(access_token_response.cookies)

    def _rate_limit(self):
        """Enforce strict rate limiting (3 requests/second)"""
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.RATE_LIMIT_WINDOW / self.RATE_LIMIT:
                delay = (self.RATE_LIMIT_WINDOW / self.RATE_LIMIT) - elapsed
                time.sleep(delay)
            self.last_request_time = time.time()

    def _format_date(self, date: Union[str, datetime]) -> str:
        """Convert date to YYYY-MM-DD format"""
        if isinstance(date, datetime):
            return date.strftime("%Y-%m-%d")
        return date

    def get_historical_data(
        self,
        instrument_token: int,
        from_date: Union[str, datetime],
        to_date: Union[str, datetime],
        interval: str = "day",
        oi: bool = True,
        continuous: bool = False,
        max_retries: int = 3,
    ) -> Dict:
        """
        Fetch historical data for an instrument with proper authentication

        Args:
            `instrument_token`: Zerodha instrument token (e.g., 1793 for NIFTY 50)
            `from_date`: Start date (YYYY-MM-DD or datetime)
            `to_date`: End date (YYYY-MM-DD or datetime)
            `interval`: Time interval (minute · day · 3minute · 5minute · 10minute · 15minute · 30minute · 60minute) . See https://kite.trade/docs/connect/v3/historical/
            `oi`: Include open interest data
            `continuous`: For continuous contracts. It's important to note that the exchanges
                          flush the instrument_token for futures and options contracts for every expiry.
                          For instance, NIFTYJAN18FUT and NIFTYFEB18FUT will have different instrument tokens
                          although their underlying contract is the same. The instrument master API only returns
                          instrument_tokens for contracts that are live. It is not possible to retrieve
                          instrument_tokens for expired contracts from the API, unless you regularly download
                          and cache them. This is where continuous API comes in which works for NFO and
                          MCX futures contracts. Given a live contract's instrument_token, the API will
                          return day candle records for the same instrument's expired contracts. For instance,
                          assuming the current month is January and you pass NIFTYJAN18FUT's
                          instrument_token along with continuous=1, you can fetch day candles for December,
                          November ... contracts by simply changing the from and to dates.

        Returns:
            Dictionary with historical data in Kite format. The response is an array of records, where each
            record in turn is an array of the following values — [timestamp, open, high, low, close, volume, oi].

        Raises:
            requests.exceptions.RequestException: After all retries fail
        """
        if instrument_token is None or len(str(instrument_token)) == 0:
            raise ValueError("instrument_token is a MUST have to work for this API")
        if from_date is None or len(from_date) == 0:
            from_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime() - datetime.timedelta(days=365)
            )
        if to_date is None or len(to_date) == 0:
            to_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime()
            )
        params = {
            "user_id": self.user_id,
            "oi": "1" if oi else "0",
            "from": self._format_date(from_date),
            "to": self._format_date(to_date),
            "continuous": "1" if continuous else "0",
        }

        url = f"{self.BASE_URL}/{instrument_token}/{interval}"

        last_error = None

        for attempt in range(max_retries):
            try:
                self._rate_limit()  # Strict rate limiting
                response = self.session.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                continue

        raise requests.exceptions.RequestException(
            f"Failed after {max_retries} attempts for {instrument_token}: {str(last_error)}"
        )

    def get_multiple_instruments(
        self,
        instruments: List[int],
        from_date: Union[str, datetime],
        to_date: Union[str, datetime],
        interval: str = "day",
        oi: bool = True,
        batch_size: int = 3,
        max_retries: int = 2,
        delay: float = 1.0,
    ) -> Dict[int, Dict]:
        """
        Fetch historical data for multiple instruments with rate limiting

        Args:
            instruments: List of instrument tokens
            from_date: Start date
            to_date: End date
            interval: Time interval (minute · day · 3minute · 5minute · 10minute · 15minute · 30minute · 60minute) . See https://kite.trade/docs/connect/v3/historical/
            oi: Include open interest
            batch_size: Number of requests per batch
            max_retries: Retry attempts per instrument
            delay: Delay between batches in seconds

        Returns:
            Dictionary mapping instrument tokens to their historical data
        """
        if instruments is None or len(instruments) == 0:
            raise ValueError("list of instruments is a MUST have to work for this API")
        if from_date is None or len(from_date) == 0:
            from_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime() - datetime.timedelta(days=365)
            )
        if to_date is None or len(to_date) == 0:
            to_date = PKDateUtilities.YmdStringFromDate(
                PKDateUtilities.currentDateTime()
            )
        results = {}
        batch_size = min(batch_size, self.RATE_LIMIT)  # Never exceed rate limit
        for i in range(0, len(instruments), batch_size):
            batch = instruments[i : i + batch_size]
            for instrument in batch:
                try:
                    results[instrument] = self.get_historical_data(
                        instrument_token=instrument,
                        from_date=from_date,
                        to_date=to_date,
                        interval=interval,
                        oi=oi,
                        max_retries=max_retries,
                    )
                except Exception as e:
                    results[instrument] = {
                        "status": "failed",
                        "data": {"candles": []},
                        "error": str(e),
                    }

            if i + batch_size < len(instruments):
                time.sleep(delay)  # Respect rate limits

        return results


"""
# First authenticate
from pkbrokers.kite.authenticator import KiteAuthenticator
authenticator = KiteAuthenticator()
enctoken = authenticator.get_enctoken(...)  # Your credentials

# Create history client with the full response object
history = KiteTickerHistory(
    enctoken=enctoken,
    user_id="whatever",
    access_token_response=authenticator.access_token_response
)

# Single request (automatically rate limited)
data = history.get_historical_data(
    instrument_token=1793,
    from_date="2024-08-10",
    to_date="2025-08-11",
    interval="day"
)

# Batch processing (automatically respects 3req/sec limit)
batch_data = history.get_multiple_instruments(
    instruments=[256265, 260105, 1793, 11536],
    from_date="2024-01-01",
    to_date="2024-01-31",
    interval="5minute"
)
"""
