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

"""
Kite Connect Authentication Module

Provides secure authentication with Zerodha's Kite Connect API
"""
import os
import uuid
from typing import Dict, Optional

import pyotp
import requests
from kiteconnect import KiteConnect
from PKDevTools.classes.Environment import PKEnvironment
from PKDevTools.classes.log import default_logger

from pkbrokers.envupdater import env_update_context


class KiteAuthenticator:
    """
    Handles authentication with Zerodha's Kite Connect API

    Usage:
        authenticator = KiteAuthenticator()
        enctoken = authenticator.get_enctoken(
            api_key="your_api_key",
            username="your_username",
            password="your_password",
            totp_secret="your_totp_secret"
        )
    """

    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "X-Kite-Version": "3.0.0",
    }

    def __init__(self, timeout: int = 30):
        """
        Initialize authenticator with optional timeout

        Args:
            timeout: Request timeout in seconds (default: 30)
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.login_url = None
        self.request_session_id_response = None
        self.request_token_response = None
        self.access_token_response = None

    def _get_login_url(self, api_key: str) -> str:
        """Get Kite Connect login URL"""
        return KiteConnect(api_key=api_key).login_url()

    def _extract_enctoken(self, cookies: Optional[str]) -> str:
        """Extract enctoken from response cookies"""
        if cookies is None:
            return ""

        for cookie in cookies.split(";"):
            if "enctoken" in cookie:
                # Handle cases where cookies might be comma-separated
                for part in cookie.strip().split(","):
                    if "enctoken" in part:
                        return part.strip().replace("enctoken=", "")
        return ""

    def _validate_credentials(self, credentials: Dict[str, str]) -> None:
        """Validate required credentials"""
        required_keys = ["api_key", "username", "password", "totp"]
        missing = [key for key in required_keys if key not in credentials]
        if missing:
            raise ValueError(f"Missing required credentials: {missing}")
        default_logger().debug(
            "Credentials verified. All required credentials are available."
        )

    def get_enctoken(self, **credentials) -> str:
        """
        Authenticate with Kite Connect and return enctoken

        Args:
            api_key: Kite Connect API key
            username: Zerodha username
            password: Zerodha password
            totp: TOTP secret key

        Returns:
            enctoken string for API authentication

        Raises:
            ValueError: If credentials are missing or invalid
            requests.exceptions.RequestException: On network/API errors
        """
        try:
            if credentials is None or len(credentials.keys()) == 0:
                default_logger().debug(
                    "Credentials not sent for authentication. Using the default credentials from environment."
                )

                local_secrets = PKEnvironment().allSecrets
                credentials = {
                    "api_key": "kitefront",
                    "username": os.environ.get(
                        "KUSER",
                        local_secrets.get("KUSER", "You need your Kite username"),
                    ),
                    "password": os.environ.get(
                        "KPWD", local_secrets.get("KPWD", "You need your Kite password")
                    ),
                    "totp": os.environ.get(
                        "KTOTP", local_secrets.get("KTOTP", "You need your Kite TOTP")
                    ),
                }
            self._validate_credentials(credentials)
        except Exception as e:
            default_logger().error(e)
            pass

        try:
            # Initial request to establish session
            self.login_url = self._get_login_url(credentials["api_key"])
            default_logger().debug(f"Login URL retrieved: {self.login_url}")
            self.request_session_id_response = self.session.get(
                self.login_url, headers=self.DEFAULT_HEADERS, timeout=self.timeout
            )
            default_logger().debug(
                f"Login response received: {self.request_session_id_response}"
            )
            # User login
            self.request_token_response = self.session.post(
                "https://kite.zerodha.com/api/login",
                data={
                    "user_id": credentials["username"],
                    "password": credentials["password"],
                    "type": "user_id",
                },
                headers=self.DEFAULT_HEADERS,
                timeout=self.timeout,
            )
            login_data = self.request_token_response.json()
            default_logger().debug(f"Going for OTP. Login Done: {login_data}")
            # TOTP verification
            totp_headers = {
                **self.DEFAULT_HEADERS,
                "Origin": "https://kite.zerodha.com",
                "Referer": "https://kite.zerodha.com/",
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Kite-Userid": credentials["username"].upper(),
                "X-Kite-App-Uuid": str(uuid.uuid4()),
            }

            self.access_token_response = self.session.post(
                "https://kite.zerodha.com/api/twofa",
                data={
                    "user_id": credentials["username"],
                    "request_id": login_data["data"]["request_id"],
                    "twofa_value": pyotp.TOTP(credentials["totp"]).now(),
                    "twofa_type": "totp",
                },
                headers=totp_headers,
                timeout=self.timeout,
            )
            default_logger().debug(
                f"OTP verification finished: {self.access_token_response}"
            )
            access_token = self._extract_enctoken(
                self.access_token_response.headers.get("Set-Cookie")
            )
            prev_token = PKEnvironment().KTOKEN
            os.environ["KTOKEN"] = access_token
            default_logger().debug(f"Token extracted: {access_token}")
            with env_update_context(os.path.join(os.getcwd(), ".env.dev")) as updater:
                updater.update_values({"KTOKEN": access_token})
                updater.reload_env()
                default_logger().debug(
                    f"Token updated in os.environment: {PKEnvironment().KTOKEN}"
                )
            try:
                from PKDevTools.classes.GitHubSecrets import PKGitHubSecretsManager

                default_logger().info(
                    f"CI_PAT length:{len(PKEnvironment().CI_PAT)}. Value: {PKEnvironment().CI_PAT[:10]}"
                )
                gh_manager = PKGitHubSecretsManager(
                    repo="pkbrokers", token=PKEnvironment().CI_PAT
                )
                gh_manager.create_or_update_secret("KTOKEN", PKEnvironment().KTOKEN)
                default_logger().info(
                    f"Token updated in GitHub secrets:{prev_token != PKEnvironment().KTOKEN}"
                )
            except Exception as e:
                default_logger().error(f"Error while updating GitHub secret:{e}")
                pass
            return access_token

        except requests.exceptions.RequestException as e:
            default_logger().error(f"RequestException:Authentication failed:{e}")
            raise requests.exceptions.RequestException(
                f"Authentication failed: {str(e)}"
            ) from e
        except Exception as e:
            default_logger().error(f"Authentication failed because of error:{e}")
            raise ValueError(f"Authentication error: {str(e)}") from e
