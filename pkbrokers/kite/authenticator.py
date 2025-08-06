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
import requests
import pyotp
from typing import Dict, Optional
from kiteconnect import KiteConnect

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
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'X-Kite-Version': '3.0.0'
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
        if credentials is None or len(credentials.keys()) == 0:
            from dotenv import dotenv_values
            from PKDevTools.classes import Archiver
            local_secrets = dotenv_values(".env.dev")
            credentials = {
                            "api_key" : "kitefront",
                            "username" : os.environ.get("KUSER",local_secrets.get("KUSER","You need your Kite username")),
                            "password" : os.environ.get("KPWD",local_secrets.get("KPWD","You need your Kite password")),
                            "totp" : os.environ.get("KTOTP",local_secrets.get("KTOTP","You need your Kite TOTP")),
                        }
        self._validate_credentials(credentials)
        
        try:
            # Initial request to establish session
            self.login_url = self._get_login_url(credentials["api_key"])
            self.request_session_id_response = self.session.get(
                self.login_url,
                headers=self.DEFAULT_HEADERS,
                timeout=self.timeout
            )
            
            # User login
            self.request_token_response = self.session.post(
                "https://kite.zerodha.com/api/login",
                data={
                    "user_id": credentials["username"],
                    "password": credentials["password"],
                    "type": "user_id"
                },
                headers=self.DEFAULT_HEADERS,
                timeout=self.timeout
            )
            login_data = self.request_token_response.json()
            
            # TOTP verification
            totp_headers = {
                **self.DEFAULT_HEADERS,
                'Origin': 'https://kite.zerodha.com',
                'Referer': 'https://kite.zerodha.com/',
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Kite-Userid': credentials["username"].upper(),
                'X-Kite-App-Uuid': str(uuid.uuid4())
            }
            
            self.access_token_response = self.session.post(
                "https://kite.zerodha.com/api/twofa",
                data={
                    "user_id": credentials["username"],
                    "request_id": login_data["data"]["request_id"],
                    "twofa_value": pyotp.TOTP(credentials["totp"]).now(),
                    "twofa_type": "totp"
                },
                headers=totp_headers,
                timeout=self.timeout
            )
            
            access_token = self._extract_enctoken(self.access_token_response.headers.get("Set-Cookie"))
            os.environ["KTOKEN"] = access_token
            return access_token
            
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(
                f"Authentication failed: {str(e)}"
            ) from e
        except Exception as e:
            raise ValueError(f"Authentication error: {str(e)}") from e

"""
# Example Usage:
if __name__ == "__main__":
    # Configuration - load from environment in production
    from dotenv import dotenv_values
    local_secrets = dotenv_values(".env.dev")
    credentials = {
                    "api_key" : "kitefront",
                    "username" : os.environ.get("KUSER",local_secrets.get("KUSER","You need your Kite username")),
                    "password" : os.environ.get("KPWD",local_secrets.get("KPWD","You need your Kite password")),
                    "totp" : os.environ.get("KTOTP",local_secrets.get("KTOTP","You need your Kite TOTP")),
                   }
    authenticator = KiteAuthenticator(timeout=10)
    req_token = authenticator.get_enctoken(**credentials)
    print(req_token)
"""